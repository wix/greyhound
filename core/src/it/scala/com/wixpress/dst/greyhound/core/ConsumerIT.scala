package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.ConsumerIT._
import com.wixpress.dst.greyhound.core.consumer.{ConsumerSpec, Consumers}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig}
import com.wixpress.dst.greyhound.core.serialization.{Deserializer, Serializer}
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, MessagesSink}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import zio.Managed
import zio.blocking.Blocking
import zio.console.Console
import zio.duration._

class ConsumerIT extends BaseTest[GreyhoundMetrics with Blocking with Console] {

  override def env: Managed[Nothing, GreyhoundMetrics with Blocking with Console] =
    Managed.succeed(new GreyhoundMetric.Live with Blocking.Live with Console.Live)

  "Greyhound consumer" should {
    "produce and consume a single message" in {
      val resources = for {
        kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
        producer <- Producer.make(ProducerConfig(kafka.bootstrapServers), serializer, serializer)
      } yield (kafka, producer)

      resources.use {
        case (kafka, producer) =>
          for {
            _ <- kafka.createTopic(topicConfig)
            sink <- MessagesSink.make[String, String]()
            spec <- ConsumerSpec.make(topic, group, sink.handler, deserializer, deserializer)
            _ <- Consumers.start(kafka.bootstrapServers, spec).fork
            _ <- producer.produce(topic, "foo", "bar")
            message <- sink.firstMessage
          } yield message must (beRecordWithKey("foo") and beRecordWithValue("bar"))
      }
    }
  }

}

object ConsumerIT {
  val partitions = 8
  val topic = Topic[String, String]("some-topic")
  val topicConfig = TopicConfig(topic.name, partitions, 1, CleanupPolicy.Delete(1.hour))
  val serializer = Serializer(new StringSerializer)
  val deserializer = Deserializer(new StringDeserializer)
  val group = "some-group"
}
