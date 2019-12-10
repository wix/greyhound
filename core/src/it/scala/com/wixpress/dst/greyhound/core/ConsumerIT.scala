package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.ConsumerIT.{serializer, _}
import com.wixpress.dst.greyhound.core.consumer.{ConsumerSpec, Consumers, RecordHandler}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig}
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, MessagesSink}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration._

class ConsumerIT extends BaseTest[GreyhoundMetrics with Blocking with Console with Clock] {

  override def env: Managed[Nothing, GreyhoundMetrics with Blocking with Console with Clock] =
    Managed.succeed(new GreyhoundMetric.Live with Blocking.Live with Console.Live with Clock.Live)

  val resources = for {
    kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
    producer <- Producer.make(ProducerConfig(kafka.bootstrapServers))
  } yield (kafka, producer)

  val tests = resources.use {
    case (kafka, producer) =>
      val test1 = for {
        sink <- MessagesSink.make[String, String]()
        spec <- ConsumerSpec.make(topic, "group-1", sink.handler, deserializer, deserializer)
        _ <- Consumers.make(kafka.bootstrapServers, spec).useForever.fork
        _ <- producer.produce(topic, "foo", "bar", serializer, serializer)
        message <- sink.firstMessage
      } yield "produce and consume a single message" in {
        message must (beRecordWithKey("foo") and beRecordWithValue("bar"))
      }

      val test2 = for {
        _ <- kafka.createTopic(TopicConfig("some-topic-retry-1", partitions, 1, CleanupPolicy.Delete(1.hour)))
        _ <- kafka.createTopic(TopicConfig("some-topic-retry-2", partitions, 1, CleanupPolicy.Delete(1.hour)))
        _ <- kafka.createTopic(TopicConfig("some-topic-retry-3", partitions, 1, CleanupPolicy.Delete(1.hour)))

        invocations <- Ref.make(0)
        promise <- Promise.make[Nothing, Unit]
        spec <- ConsumerSpec.makeWithRetry[Any, String, String](
          topic = Topic("some-topic"),
          group = "group-2",
          handler = RecordHandler { _ =>
            invocations.update(_ + 1).flatMap { n =>
              if (n <= 4) ZIO.fail(new RuntimeException("Oops!"))
              else promise.succeed(())
            }
          },
          keyDeserializer = deserializer,
          valueDeserializer = deserializer,
          retryPolicy = Vector(1.second, 2.seconds, 3.seconds),
          producer = producer)
        _ <- Consumers.make(kafka.bootstrapServers, spec).useForever.fork
        _ <- producer.produce(topic, "foo", "bar", serializer, serializer)
        success <- promise.await.timeout(8.seconds)
      } yield "configure a handler with retry policy" in {
        success must beSome
      }

      kafka.createTopic(topicConfig) *>
        all(test1, test2)
  }

  run(tests)

}

object ConsumerIT {
  val partitions = 8
  val topic = Topic[String, String]("some-topic")
  val topicConfig = TopicConfig(topic.name, partitions, 1, CleanupPolicy.Delete(1.hour))
  val serializer = Serializer(new StringSerializer)
  val deserializer = Deserializer(new StringDeserializer)
}
