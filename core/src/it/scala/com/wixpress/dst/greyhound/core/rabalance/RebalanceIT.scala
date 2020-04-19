package com.wixpress.dst.greyhound.core.rabalance

import com.wixpress.dst.greyhound.core.ConsumerIT.{Env, randomGroup, createRandomTopic, testResources}
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, CountDownLatch}
import com.wixpress.dst.greyhound.core.{ConsumerIT, Group, Topic}
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import zio._

class RebalanceIT extends BaseTest[Env] {
  sequential

  override def env: UManaged[Env] = ConsumerIT.ManagedEnv

  val resources = testResources()

  val tests = resources.use {
    case (kafka, producer) =>
      implicit val _kafka: ManagedKafka = kafka

      def verifyGroupCommitted(topic: Topic, group: Group, partitions: Int) = for {
        latch <- CountDownLatch.make(partitions)
        handler = RecordHandler(topic)((_: ConsumerRecord[Chunk[Byte], Chunk[Byte]]) => latch.countDown)
        _ <- RecordConsumer.make(RecordConsumerConfig(kafka.bootstrapServers, group), handler).use_ {
          ZIO.foreachPar_(0 until partitions) { partition =>
            producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(partition)))
          } *> latch.await
        }
      } yield ()

      val commitOnRebalanceTest = for {
        topic <- createRandomTopic()
        group <- randomGroup

        allMessages = 400
        someMessages = 100
        produce = producer.produce(ProducerRecord(topic, Chunk.empty))

        invocations <- Ref.make(0)
        handledAll <- CountDownLatch.make(allMessages)
        handledSome <- CountDownLatch.make(someMessages)
        handler = RecordHandler(topic) { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          invocations.update(_ + 1) *>
            handledSome.countDown *>
            handledAll.countDown
        }
        consumer = RecordConsumer.make(RecordConsumerConfig(kafka.bootstrapServers, group), handler)

        startProducing1 <- Promise.make[Nothing, Unit]
        consumer1 <- consumer.use_(startProducing1.succeed(()) *> handledAll.await).fork
        _ <- startProducing1.await *> ZIO.foreachPar(0 until someMessages)(_ => produce)

        _ <- handledSome.await
        startProducing2 <- Promise.make[Nothing, Unit]
        consumer2 <- consumer.use_(startProducing2.succeed(()) *> handledAll.await).fork
        _ <- startProducing2.await *> ZIO.foreachPar(someMessages until allMessages)(_ => produce)

        _ <- consumer1.join
        _ <- consumer2.join
        allInvocations <- invocations.get
      } yield "don't reprocess messages after rebalance" in {
        allInvocations must equalTo(allMessages)
      }

      val gracefulRebalanceTest = for {
        group <- randomGroup

        partitions = 2
        messagesPerPartition = 2
        topic <- createRandomTopic(partitions = partitions)
        _ <- verifyGroupCommitted(topic, group, partitions)

        latch <- CountDownLatch.make(messagesPerPartition * partitions)
        blocker <- Promise.make[Nothing, Unit]
        semaphore <- Semaphore.make(1)
        handler1 = RecordHandler(topic) { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          semaphore.withPermit(blocker.await *> latch.countDown)
        }

        startProducing <- Promise.make[Nothing, Unit]
        config1 = RecordConsumerConfig(
          bootstrapServers = kafka.bootstrapServers,
          clientId = "client-1",
          group = group,
          eventLoopConfig = EventLoopConfig.Default.copy(
            rebalanceListener = new RebalanceListener[Any] {
              override def onPartitionsRevoked(partitions: Set[TopicPartition]): UIO[Any] =
                ZIO.when(partitions.nonEmpty)(blocker.succeed(()))

              override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] =
                ZIO.unit
            }))
        consumer1 <- RecordConsumer.make(config1, handler1).use_ {
          startProducing.succeed(()) *> latch.await
        }.fork

        _ <- startProducing.await
        _ <- ZIO.foreachPar(0 until messagesPerPartition) { _ =>
          producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(0))) zipPar
            producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(1)))
        }

        handler2 = RecordHandler(topic) { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          latch.countDown
        }
        config2 = RecordConsumerConfig(kafka.bootstrapServers, group, "client-2")
        consumer2 <- RecordConsumer.make(config2, handler2).use_ {
          latch.await
        }.fork

        _ <- consumer1.join
        _ <- consumer2.join
        handled <- latch.count
      } yield "drain queues and finish executing tasks on rebalance" in {
        handled must equalTo(0)
      }

      all(
        commitOnRebalanceTest,
        gracefulRebalanceTest)
  }

  run(tests)

}

