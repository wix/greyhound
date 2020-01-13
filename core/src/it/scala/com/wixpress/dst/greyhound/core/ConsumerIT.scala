package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.ConsumerIT._
import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord}
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, CountDownLatch}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.random.Random

class ConsumerIT extends BaseTest[GreyhoundMetrics with Blocking with Console with Clock with Random] {

  type Env = GreyhoundMetrics with Blocking with Console with Clock with Random

  override def env: UManaged[Env] =
    Managed.succeed(new GreyhoundMetric.Live with Blocking.Live with Console.Live with Clock.Live with Random.Live)

  val resources = for {
    kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
    producer <- Producer.make(ProducerConfig(kafka.bootstrapServers))
  } yield (kafka, producer)

  val tests = resources.use {
    case (kafka, producer) =>
      def randomTopic(partitions: Int = ConsumerIT.partitions) = for {
        topic <- randomId.map(id => s"topic-$id")
        _ <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete))
      } yield topic

      val test1 = for {
        topic <- randomTopic()
        group <- randomGroup

        queue <- Queue.unbounded[ConsumerRecord[String, String]]
        offsets <- Offsets.make

        eventLoop = for {
          consumer <- Consumer.make(ConsumerConfig(kafka.bootstrapServers, group, clientId))
          handler <- RecordHandler(topic)(queue.offer)
            .deserialize(StringSerde, StringSerde)
            .andThen(offsets.update)
            .parallel(partitions)
          eventLoop <- EventLoop.make[Env](consumer, offsets, handler.ignore)
        } yield eventLoop

        _ <- eventLoop.useForever.fork // TODO when is consumer ready?
        _ <- producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde)
        message <- queue.take
      } yield "produce and consume a single message" in {
        message must (beRecordWithKey("foo") and beRecordWithValue("bar"))
      }

      val test2 = for {
        topic <- randomTopic()
        group <- randomGroup
        _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-0", partitions, 1, delete))
        _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-1", partitions, 1, delete))
        _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-2", partitions, 1, delete))

        offsets <- Offsets.make
        invocations <- Ref.make(0)
        done <- Promise.make[Nothing, Unit]
        retryPolicy = RetryPolicy.default(topic, group, 1.second, 2.seconds, 3.seconds)
        handler = RecordHandler(topic) { _: ConsumerRecord[String, String] =>
          invocations.update(_ + 1).flatMap { n =>
            if (n < 4) ZIO.fail(new RuntimeException("Oops!"))
            else done.succeed(()) // Succeed on final retry
          }
        }

        eventLoop = for {
          consumer <- Consumer.make(ConsumerConfig(kafka.bootstrapServers, group, clientId))
          retryHandler = handler
            .deserialize(StringSerde, StringSerde)
            .withRetries(retryPolicy, producer)
            .andThen(offsets.update)
            .ignore
          eventLoop <- EventLoop.make[Env](consumer, offsets, retryHandler)
        } yield eventLoop

        _ <- eventLoop.useForever.fork
        _ <- producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde)
        success <- done.await.timeout(8.seconds)
      } yield "configure a handler with retry policy" in {
        success must beSome
      }

      val test3 = for {
        topic1 <- randomTopic()
        topic2 <- randomTopic()
        group <- randomGroup

        records1 <- Queue.unbounded[ConsumerRecord[String, String]]
        records2 <- Queue.unbounded[ConsumerRecord[Int, Int]]
        offsets <- Offsets.make

        eventLoop = for {
          consumer <- Consumer.make(ConsumerConfig(kafka.bootstrapServers, group, clientId))
          handler1 <- RecordHandler(topic1)(records1.offer).andThen(offsets.update).parallel(partitions)
          handler2 <- RecordHandler(topic2)(records2.offer).andThen(offsets.update).parallel(partitions)
          handler = handler1.deserialize(StringSerde, StringSerde) combine handler2.deserialize(IntSerde, IntSerde)
          eventLoop <- EventLoop.make[Env](consumer, offsets, handler.ignore)
        } yield eventLoop

        _ <- eventLoop.useForever.fork
        _ <- producer.produce(ProducerRecord(topic1, "bar", Some("foo")), StringSerde, StringSerde)
        _ <- producer.produce(ProducerRecord(topic2, 2, Some(1)), IntSerde, IntSerde)
        record1 <- records1.take
        record2 <- records2.take
      } yield "consume messages from combined handlers" in {
        (record1 must (beRecordWithKey("foo") and beRecordWithValue("bar"))) and
          (record2 must (beRecordWithKey(1) and beRecordWithValue(2)))
      }

      val test4 = for {
        partitions <- ZIO.succeed(2)
        topic <- randomTopic(partitions)
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

        offsets <- Offsets.make
        eventLoop = for {
          consumer <- Consumer.make(ConsumerConfig(kafka.bootstrapServers, group, clientId))
          handler <- handler.andThen(offsets.update).ignore.parallel(partitions)
          eventLoop <- EventLoop.make[Env](consumer, offsets, handler, handler)
        } yield eventLoop
        _ <- eventLoop.useForever.fork

        record = ProducerRecord(topic, Chunk.empty)
        _ <- ZIO.foreachPar(0 until messagesPerPartition) { _ =>
          producer.produce(record.copy(partition = Some(0))) zipPar
            producer.produce(record.copy(partition = Some(1)))
        }

        handledAllFromPartition0 <- handledPartition0.await.timeout(10.seconds)
        _ <- delayPartition1.succeed(())
        handledAllFromPartition1 <- handledPartition1.await.timeout(10.seconds)
      } yield "not lose any messages on a slow consumer (drives the message dispatcher to throttling)" in {
        (handledAllFromPartition0 must beSome) and (handledAllFromPartition1 must beSome)
      }

      all(test1, test2, test3, test4)
  }

  run(tests)

}

object ConsumerIT {
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
