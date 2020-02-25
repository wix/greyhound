package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.ConsumerIT._
import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord, ReportingProducer}
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, CountDownLatch}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.random.Random

class ConsumerIT extends BaseTest[Env] {

  override def env: UManaged[Env] =
    Managed.succeed {
      new GreyhoundMetric.Live
        with Blocking.Live
        with Console.Live
        with Clock.Live
        with Random.Live
    }

  val resources = for {
    kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
    producer <- Producer.make(ProducerConfig(kafka.bootstrapServers))
  } yield (kafka, ReportingProducer(producer))

  val tests = resources.use {
    case (kafka, producer) =>
      def randomTopic(partitions: Int = ConsumerIT.partitions) = for {
        topic <- randomId.map(id => s"topic-$id")
        _ <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete))
      } yield topic

      def verifyGroupCommitted(topic: Topic, group: Group, partitions: Int) = for {
        latch <- CountDownLatch.make(partitions)
        handler = RecordHandler(topic)((_: ConsumerRecord[Chunk[Byte], Chunk[Byte]]) => latch.countDown)
        _ <- ParallelConsumer.make(kafka.bootstrapServers, group -> handler).use_ {
          ZIO.foreachPar_(0 until partitions) { partition =>
            producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(partition)))
          } *> latch.await
        }
      } yield ()

      val simpleTest = for {
        topic <- randomTopic()
        group <- randomGroup

        queue <- Queue.unbounded[ConsumerRecord[String, String]]
        handler = RecordHandler(topic)(queue.offer(_: ConsumerRecord[String, String]))
          .withDeserializers(StringSerde, StringSerde)
          .ignore

        message <- ParallelConsumer.make(kafka.bootstrapServers, group -> handler).use_ {
          val record = ProducerRecord(topic, "bar", Some("foo"))
          producer.produce(record, StringSerde, StringSerde) *>
            queue.take
        }
      } yield "produce and consume a single message" in {
        message must (beRecordWithKey("foo") and beRecordWithValue("bar"))
      }

      val combinedHandlersTest = for {
        topic1 <- randomTopic()
        topic2 <- randomTopic()
        group <- randomGroup

        records1 <- Queue.unbounded[ConsumerRecord[String, String]]
        records2 <- Queue.unbounded[ConsumerRecord[Int, Int]]
        handler1 = RecordHandler(topic1)(records1.offer(_: ConsumerRecord[String, String]))
        handler2 = RecordHandler(topic2)(records2.offer(_: ConsumerRecord[Int, Int]))
        handler = handler1.withDeserializers(StringSerde, StringSerde) combine handler2.withDeserializers(IntSerde, IntSerde)

        (record1, record2) <- ParallelConsumer.make(kafka.bootstrapServers, group -> handler.ignore).use_ {
          producer.produce(ProducerRecord(topic1, "bar", Some("foo")), StringSerde, StringSerde) *>
            producer.produce(ProducerRecord(topic2, 2, Some(1)), IntSerde, IntSerde) *>
            (records1.take zip records2.take)
        }
      } yield "consume messages from combined handlers" in {
        (record1 must (beRecordWithKey("foo") and beRecordWithValue("bar"))) and
          (record2 must (beRecordWithKey(1) and beRecordWithValue(2)))
      }

      val throttlingTest = for {
        topic <- randomTopic(partitions = 2)
        group <- randomGroup

        messagesPerPartition = 500 // Exceeds queue's capacity
        delayPartition1 <- Promise.make[Nothing, Unit]
        handledPartition0 <- CountDownLatch.make(messagesPerPartition)
        handledPartition1 <- CountDownLatch.make(messagesPerPartition)
        handler = RecordHandler(topic) { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          record.partition match {
            case 0 => handledPartition0.countDown
            case 1 => delayPartition1.await *> handledPartition1.countDown
          }
        }

        test <- ParallelConsumer.make(kafka.bootstrapServers, group -> handler).use_ {
          val recordPartition0 = ProducerRecord(topic, Chunk.empty, partition = Some(0))
          val recordPartition1 = ProducerRecord(topic, Chunk.empty, partition = Some(1))
          for {
            _ <- ZIO.foreachPar(0 until messagesPerPartition) { _ =>
              producer.produce(recordPartition0) zipPar producer.produce(recordPartition1)
            }

            handledAllFromPartition0 <- handledPartition0.await.timeout(5.seconds)
            _ <- delayPartition1.succeed(())
            handledAllFromPartition1 <- handledPartition1.await.timeout(5.seconds)
          } yield "not lose any messages on a slow consumer (drives the message dispatcher to throttling)" in {
            (handledAllFromPartition0 must beSome) and (handledAllFromPartition1 must beSome)
          }
        }
      } yield test

      val pauseResumeTest = for {
        topic <- randomTopic()
        group <- randomGroup

        numberOfMessages = 32
        someMessages = 16
        restOfMessages = numberOfMessages - someMessages
        handledSomeMessages <- CountDownLatch.make(someMessages)
        handledAllMessages <- CountDownLatch.make(numberOfMessages)
        handler = RecordHandler(topic) { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          handledSomeMessages.countDown zipParRight handledAllMessages.countDown
        }

        test <- ParallelConsumer.make(kafka.bootstrapServers, group -> handler).use { consumer =>
          val record = ProducerRecord(topic, Chunk.empty)
          for {
            _ <- ZIO.foreachPar(0 until someMessages)(_ => producer.produce(record))
            _ <- handledSomeMessages.await
            _ <- consumer.pause
            _ <- ZIO.foreachPar(0 until restOfMessages)(_ => producer.produce(record))
            a <- handledAllMessages.await.timeout(5.seconds)
            _ <- consumer.resume
            b <- handledAllMessages.await.timeout(5.seconds)
          } yield "pause and resume consumer" in {
            (a must beNone) and (b must beSome)
          }
        }
      } yield test

      val gracefulShutdownTest = for {
        topic <- randomTopic()
        group <- randomGroup

        ref <- Ref.make(0)
        startedHandling <- Promise.make[Nothing, Unit]
        handler = RecordHandler(topic) { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          startedHandling.succeed(()) *>
            clock.sleep(5.seconds) *>
            ref.update(_ + 1)
        }

        _ <- ParallelConsumer.make(kafka.bootstrapServers, group -> handler).use_ {
          producer.produce(ProducerRecord(topic, Chunk.empty)) *>
            startedHandling.await
        }

        handled <- ref.get
      } yield "wait until queues are drained" in {
        handled must equalTo(1)
      }

      val commitOnRebalanceTest = for {
        topic <- randomTopic()
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
        consumer = ParallelConsumer.make(kafka.bootstrapServers, group -> handler)

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
        topic <- randomTopic(partitions = partitions)
        _ <- verifyGroupCommitted(topic, group, partitions)

        latch <- CountDownLatch.make(messagesPerPartition * partitions)
        blocker <- Promise.make[Nothing, Unit]
        semaphore <- Semaphore.make(1)
        handler1 = RecordHandler(topic) { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          semaphore.withPermit(blocker.await *> latch.countDown)
        }

        startProducing <- Promise.make[Nothing, Unit]
        config1 = ParallelConsumerConfig(
          bootstrapServers = kafka.bootstrapServers,
          clientId = "client-1",
          eventLoopConfig = EventLoopConfig.Default.copy(
            rebalanceListener = new RebalanceListener[Any] {
              override def onPartitionsRevoked(partitions: Set[TopicPartition]): UIO[Any] =
                ZIO.when(partitions.nonEmpty)(blocker.succeed(()))

              override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] =
                ZIO.unit
            }))
        consumer1 <- ParallelConsumer.make(config1, Map(group -> handler1)).use_ {
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
        config2 = ParallelConsumerConfig(kafka.bootstrapServers, "client-2")
        consumer2 <- ParallelConsumer.make(config2, Map(group -> handler2)).use_ {
          latch.await
        }.fork

        _ <- consumer1.join
        _ <- consumer2.join
        handled <- latch.count
      } yield "drain queues and finish executing tasks on rebalance" in {
        handled must equalTo(0)
      }

      val retryTest = for {
        topic <- randomTopic()
        group <- randomGroup
        _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-0", partitions, 1, delete))
        _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-1", partitions, 1, delete))
        _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-2", partitions, 1, delete))

        invocations <- Ref.make(0)
        done <- Promise.make[Nothing, Unit]
        retryPolicy = RetryPolicy.default(topic, group, 1.second, 2.seconds, 3.seconds)
        handler = RecordHandler(topic) { _: ConsumerRecord[String, String] =>
          invocations.update(_ + 1).flatMap { n =>
            if (n < 4) ZIO.fail(new RuntimeException("Oops!"))
            else done.succeed(()) // Succeed on final retry
          }
        }
        retryHandler = handler
          .withDeserializers(StringSerde, StringSerde)
          .withRetries(retryPolicy, producer)
          .ignore

        success <- ParallelConsumer.make(kafka.bootstrapServers, group -> retryHandler).use_ {
          producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *>
            done.await.timeout(1.minute)
        }
      } yield "configure a handler with retry policy" in {
        success must beSome
      }

      all(
        simpleTest,
        combinedHandlersTest,
        throttlingTest,
        pauseResumeTest,
        gracefulShutdownTest,
        commitOnRebalanceTest,
        gracefulRebalanceTest,
        retryTest)
  }

  run(tests)

}

object ConsumerIT {
  type Env = GreyhoundMetrics with Blocking with Console with Clock with Random

  val clientId = "greyhound-consumers"
  val partitions = 8
  val delete = CleanupPolicy.Delete(1.hour)
  val randomAlphaLowerChar = {
    val low = 97
    val high = 122
    random.nextInt(high - low).map(i => (i + low).toChar)
  }
  val randomId = ZIO.collectAll(List.fill(6)(randomAlphaLowerChar)).map(_.mkString)
  val randomGroup = randomId.map(id => s"group-$id")
}
