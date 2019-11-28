package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.ConsumerIT._
import com.wixpress.dst.greyhound.core.consumer.{ConsumerSpec, Consumers}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig}
import com.wixpress.dst.greyhound.core.serialization.{Deserializer, Serializer}
import com.wixpress.dst.greyhound.core.testkit.MessagesSink
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
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
          } yield message must (recordWithKey("foo") and recordWithValue("bar"))
      }
    }

//    "parallelize message consumption per partition" in unsafeRun {
//      val resources = for {
//        kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
//        producer <- Producer.make(ProducerConfig(kafka.bootstrapServers), serializer, serializer)
//      } yield (kafka, producer)
//
//      resources.use {
//        case (kafka, producer) =>
//          for {
//            _ <- kafka.createTopic(topicConfig)
//            sink <- MessagesSink.make[String, String]()
//            handler = sink.handler *> RecordHandler(_ => clock.sleep(1.second))
//            spec <- ConsumerSpec.make(topic, group, handler, deserializer, deserializer)
//            consumers <- Consumers.start(kafka.bootstrapServers, spec).fork
//            _ <- ZIO.foreachPar(0 until partitions) { partition =>
////              producer.produce(topic, s"key-$partition", s"value-$partition")
//              producer.produce(topic,s"value-$partition")
//            }
//            message <- sink.firstMessage
//            _ <- consumers.interrupt
//          } yield message must equalTo("foo" -> "bar")
//      }
//    }
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
