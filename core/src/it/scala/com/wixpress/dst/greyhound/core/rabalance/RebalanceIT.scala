package com.wixpress.dst.greyhound.core.rabalance

import com.wixpress.dst.greyhound.core.consumer.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, CountDownLatch}
import com.wixpress.dst.greyhound.core.{Group, Topic}
import com.wixpress.dst.greyhound.testkit.{ITEnv, ManagedKafka}
import zio.duration._
import com.wixpress.dst.greyhound.testkit.ITEnv._
import zio._

class RebalanceIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env: UManaged[Env] = ITEnv.ManagedEnv

  override def sharedEnv: ZManaged[Env, Throwable, TestResources] = testResources()

  "don't reprocess messages after rebalance" in {
    for {
      TestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(prefix = "rebalance-dont-reprocess")
      group <- randomGroup

      allMessages = 400
      someMessages = 100
      produce = producer.produce(ProducerRecord(topic, Chunk.empty))

      invocations <- Ref.make(0)
      handledAll <- CountDownLatch.make(allMessages)
      handledSome <- CountDownLatch.make(someMessages)
      handler = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        invocations.update(_ + 1) *>
          handledSome.countDown *>
          handledAll.countDown
      }
      consumer = RecordConsumer.make(configFor(topic, group, kafka).copy(offsetReset = OffsetReset.Earliest), handler)

      startProducing1 <- Promise.make[Nothing, Unit]
      consumer1 <- consumer.use_(startProducing1.succeed(()) *> handledAll.await).fork
      _ <- startProducing1.await *> ZIO.foreachPar(0 until someMessages)(_ => produce)

      _ <- handledSome.await
      startProducing2 <- Promise.make[Nothing, Unit]
      consumer2 <- consumer.use_(startProducing2.succeed(()) *> handledAll.await).fork
      _ <- startProducing2.await *> ZIO.foreachPar(someMessages until allMessages)(_ => produce)

      _ <- ZIO.foreach(Seq(consumer1, consumer2))(_.join.timeoutFail(TimeoutJoiningConsumer())(30.seconds))
      allInvocations <- invocations.get
    } yield {
      allInvocations must equalTo(allMessages)
    }
  }

  "drain queues and finish executing tasks on rebalance" in {
    for {
      TestResources(kafka, producer) <- getShared
      group <- randomGroup

      partitions = 2
      messagesPerPartition = 2
      topic <- kafka.createRandomTopic(partitions = partitions, prefix = "rebalance-drain-queues")
      _ <- verifyGroupCommitted(topic, group, partitions)

      latch <- CountDownLatch.make(messagesPerPartition * partitions)
      blocker <- Promise.make[Nothing, Unit]
      semaphore <- Semaphore.make(1)
      handler1 = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        semaphore.withPermit(blocker.await *> latch.countDown)
      }

      startProducing <- Promise.make[Nothing, Unit]
      config1 = configFor(topic, group, kafka).copy(
        clientId = "client-1",
        offsetReset = OffsetReset.Earliest,
        eventLoopConfig = EventLoopConfig.Default.copy(
          rebalanceListener = new RebalanceListener[Any] {
            override def onPartitionsRevoked(partitions: Set[TopicPartition]): UIO[Any] =
              ZIO.when(partitions.nonEmpty)(blocker.succeed(()))

            override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] =
              ZIO.unit
          }),
        extraProperties = fastConsumerMetadataFetching)
      consumer1 <- RecordConsumer.make(config1, handler1).use_ {
        startProducing.succeed(()) *> latch.await
      }.fork

      _ <- startProducing.await
      _ <- ZIO.foreachPar(0 until messagesPerPartition) { _ =>
        producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(0))) zipPar
          producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(1)))
      }

      handler2 = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        latch.countDown
      }
      config2 = configFor(topic, group, kafka).copy(clientId = "client-2")
      consumer2 <- RecordConsumer.make(config2, handler2).use_ {
        latch.await
      }.fork

      _ <- ZIO.foreach(Seq(consumer1, consumer2))(_.join.timeoutFail(TimeoutJoiningConsumer())(30.seconds))

      handled <- latch.count
    } yield {
      handled must equalTo(0)
    }
  }

  private def verifyGroupCommitted(topic: Topic, group: Group, partitions: Int) = for {
    TestResources(kafka, producer) <- getShared
    latch <- CountDownLatch.make(partitions)
    handler = RecordHandler((_: ConsumerRecord[Chunk[Byte], Chunk[Byte]]) => latch.countDown)
    _ <- RecordConsumer.make(configFor(topic, group, kafka), handler).use_ {
      ZIO.foreachPar_(0 until partitions) { partition =>
        producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(partition)))
      } *> latch.await
    }
  } yield ()

  private def configFor(topic: Topic, group: Group, kafka: ManagedKafka) =
    RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic)), extraProperties = fastConsumerMetadataFetching)

  private def fastConsumerMetadataFetching = Map("metadata.max.age.ms" -> "0")
}


case class TimeoutJoiningConsumer() extends RuntimeException