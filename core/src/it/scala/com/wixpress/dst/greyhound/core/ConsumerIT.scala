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
  sequential

  override def env: UManaged[Env] = ConsumerIT.ManagedEnv

  val resources = testResources()

  val tests = resources.use {
    case (kafka, producer) =>
      implicit val _kafka: ManagedKafka = kafka

      val simpleTest = for {
        _ <- console.putStrLn(">>>> starting test: simpleTest")

        topic <- randomTopic()
        group <- randomGroup

        queue <- Queue.unbounded[ConsumerRecord[String, String]]
        handler = RecordHandler(topic)(queue.offer(_: ConsumerRecord[String, String]))
          .withDeserializers(StringSerde, StringSerde)
          .ignore

        message <- ParallelConsumer.makeFrom(kafka.bootstrapServers, ((OffsetReset.Latest, group) -> handler)).use_ {
          val record = ProducerRecord(topic, "bar", Some("foo"))
          producer.produce(record, StringSerde, StringSerde) *>
            queue.take
        }
      } yield "produce and consume a single message" in {
        message must (beRecordWithKey("foo") and beRecordWithValue("bar"))
      }

      val throttlingTest = for {
        _ <- console.putStrLn(">>>> starting test: throttlingTest")

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

        test <- ParallelConsumer.makeFrom(kafka.bootstrapServers, ((OffsetReset.Latest, group) -> handler)).use_ {
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
        _ <- console.putStrLn(">>>> starting test: pauseResumeTest")

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

        test <- ParallelConsumer.makeFrom(kafka.bootstrapServers, ((OffsetReset.Latest, group) -> handler)).use { consumer =>
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
        _ <- console.putStrLn(">>>> starting test: gracefulShutdownTest")
        topic <- randomTopic()
        group <- randomGroup

        ref <- Ref.make(0)
        startedHandling <- Promise.make[Nothing, Unit]
        handler = RecordHandler(topic) { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
          startedHandling.succeed(()) *>
            clock.sleep(5.seconds) *>
            ref.update(_ + 1)
        }

        _ <- ParallelConsumer.makeFrom(kafka.bootstrapServers, ((OffsetReset.Latest, group) -> handler)).use_ {
          producer.produce(ProducerRecord(topic, Chunk.empty)) *>
            startedHandling.await
        }

        handled <- ref.get
      } yield "wait until queues are drained" in {
        handled must equalTo(1)
      }

      val earliestTest = for {
        _ <- console.putStrLn(">>>> starting test: earliestTest")
        topic <- randomTopic()
        group <- randomGroup

        queue <- Queue.unbounded[ConsumerRecord[String, String]]
        handler = RecordHandler(topic)(queue.offer(_: ConsumerRecord[String, String]))
          .withDeserializers(StringSerde, StringSerde)
          .ignore

        record = ProducerRecord(topic, "bar", Some("foo"))
        _ <- producer.produce(record, StringSerde, StringSerde)

        message <- ParallelConsumer.makeFrom(kafka.bootstrapServers, (OffsetReset.Earliest, group) -> handler).use_ {
          queue.take
        }.timeout(10.seconds)
      } yield "consumer from earliest offset" in {
        message.get must (beRecordWithKey("foo") and beRecordWithValue("bar"))
      }

      all(
        earliestTest,
        simpleTest,
        throttlingTest,
        pauseResumeTest,
        gracefulShutdownTest)
  }

  run(tests)

}

object ConsumerIT {
  type Env = GreyhoundMetrics with Blocking with Console with Clock with Random

  def ManagedEnv = {
    Managed.succeed {
      new GreyhoundMetric.Live
        with Blocking.Live
        with Console.Live
        with Clock.Live
        with Random.Live
    }
  }

  def testResources() = {
    for {
      kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
      producer <- Producer.make(ProducerConfig(kafka.bootstrapServers))
    } yield (kafka, ReportingProducer(producer))
  }

  def randomTopic(partitions: Int = ConsumerIT.partitions)(implicit kafka: ManagedKafka) = for {
    topic <- randomId.map(id => s"topic-$id")
    _ <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete))
  } yield topic

  val clientId = "greyhound-consumers"
  val partitions = 8
  val delete = CleanupPolicy.Delete(1.hour.toMillis)

  def randomAlphaLowerChar = {
    val low = 97
    val high = 122
    random.nextInt(high - low).map(i => (i + low).toChar)
  }

  val randomId = ZIO.collectAll(List.fill(6)(randomAlphaLowerChar)).map(_.mkString)
  val randomGroup = randomId.map(id => s"group-$id")
}
