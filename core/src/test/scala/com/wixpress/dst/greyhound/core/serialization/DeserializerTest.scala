package com.wixpress.dst.greyhound.core.serialization

import com.wixpress.dst.greyhound.core.testkit.BaseTest
import com.wixpress.dst.greyhound.core.{Deserializer, Headers, Serializer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import zio.{Managed, ZIO}

class DeserializerTest extends BaseTest[Any] {

  override def env: Managed[Nothing, Any] = Managed.unit

  val topic = "topic"
  val headers = Headers.Empty

  "map" should {
    "transform the deserialized value" in {
      val intSerializer = Serializer(new IntegerSerializer)
      val intDeserializer = Deserializer(new IntegerDeserializer)
      val stringDeserializer = intDeserializer.map(_.toString)

      for {
        serialized <- intSerializer.serialize(topic, 42)
        deserialized <- stringDeserializer.deserialize(topic, headers, serialized)
      } yield deserialized must equalTo("42")
    }
  }

  "zip" should {
    "combine 2 deserialized values into a tuple" in {
      val stringSerializer = Serializer(new StringSerializer)
      val stringDeserializer = Deserializer(new StringDeserializer)
      val topicDeserializer = Deserializer((topic, _, _) => ZIO.succeed(topic))
      val topicAndValue = topicDeserializer zip stringDeserializer
      val serialized = stringSerializer.serialize(topic, "foo")

      for {
        serialized <- stringSerializer.serialize(topic, "foo")
        deserialized <- topicAndValue.deserialize(topic, headers, serialized)
      } yield deserialized must equalTo(topic -> "foo")
    }
  }

}
