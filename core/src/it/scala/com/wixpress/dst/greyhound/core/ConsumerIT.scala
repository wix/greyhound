package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.ConsumerIT._
import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord}
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration._

class ConsumerIT extends BaseTest[GreyhoundMetrics with Blocking with Console with Clock] {

  type Env = GreyhoundMetrics with Blocking with Console with Clock

  override def env: UManaged[Env] =
    Managed.succeed(new GreyhoundMetric.Live with Blocking.Live with Console.Live with Clock.Live)

  val resources = for {
    kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
    producer <- Producer.make(ProducerConfig(kafka.bootstrapServers))
  } yield (kafka, producer)

  val tests = resources.use {
    case (kafka, producer) =>
      val test1 = for {
        topic <- ZIO.succeed("topic-1")
        _ <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete))

        queue <- Queue.unbounded[ConsumerRecord[String, String]]
        handler = RecordHandler(topic)(queue.offer)
          .deserialize(StringSerde, StringSerde)
          .ignore

        _ <- handler.parallel(8).flatMap { parallelHandler =>
          Consumers.make[Env](kafka.bootstrapServers, Map("group-1" -> parallelHandler))
        }.useForever.fork // TODO when is consumer ready?

        _ <- producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde)
        message <- queue.take
      } yield "produce and consume a single message" in {
        message must (beRecordWithKey("foo") and beRecordWithValue("bar"))
      }

      val test2 = for {
        topic <- ZIO.succeed("topic-2")
        group <- ZIO.succeed("group-2")
        _ <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete))
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
          .deserialize(StringSerde, StringSerde)
          .withRetries(retryPolicy, producer)
          .ignore
        _ <- Consumers.make[Env](kafka.bootstrapServers, Map(group -> retryHandler)).useForever.fork
        _ <- producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde)
        success <- done.await.timeout(8.seconds)
      } yield "configure a handler with retry policy" in {
        success must beSome
      }

      val test3 = for {
        topic1 <- ZIO.succeed("topic-3")
        topic2 <- ZIO.succeed("topic-4")
        group <- ZIO.succeed("group-3")
        _ <- kafka.createTopic(TopicConfig(topic1, partitions, 1, delete))
        _ <- kafka.createTopic(TopicConfig(topic2, partitions, 1, delete))

        records1 <- Queue.unbounded[ConsumerRecord[String, String]]
        records2 <- Queue.unbounded[ConsumerRecord[Int, Int]]
        offsets <- Offsets.make

        eventLoop = for {
          consumer <- Consumer.make(ConsumerConfig(kafka.bootstrapServers, group, "greyhound-consumers"))
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

      all(test1, test2, test3)
  }

  run(tests)

}

object ConsumerIT {
  val partitions = 8
  val delete = CleanupPolicy.Delete(1.hour)
}
