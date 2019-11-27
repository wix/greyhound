package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.ConsumerIT._
import com.wixpress.dst.greyhound.core.consumer.{ConsumerSpec, Consumers}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig}
import com.wixpress.dst.greyhound.core.serialization.{Deserializer, Serializer}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig, MessagesSink}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.specs2.mutable.SpecificationWithJUnit
import zio.DefaultRuntime
import zio.duration._

class ConsumerIT extends SpecificationWithJUnit with DefaultRuntime {

  "Greyhound consumer" should {
    "produce and consume a single message" in unsafeRun {
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
            consumers <- Consumers.start(kafka.bootstrapServers, spec).fork
            _ <- producer.produce(topic, "foo", "bar")
            message <- sink.firstMessage
            _ <- consumers.interrupt
          } yield message must equalTo("foo" -> "bar")
      }
    }
  }

}

object ConsumerIT {
  val topic = Topic[String, String]("some-topic")
  val topicConfig = TopicConfig(topic.name, 8, 1, CleanupPolicy.Delete(1.hour))
  val serializer = Serializer(new StringSerializer)
  val deserializer = Deserializer(new StringDeserializer)
  val group = "some-group"
}
