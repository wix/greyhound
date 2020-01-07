package com.wixpress.dst.greyhound.core.serialization

import com.wixpress.dst.greyhound.core.testkit.BaseTest
import com.wixpress.dst.greyhound.core.{Deserializer, Headers, Serializer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer}
import zio.{Managed, UManaged}

class DeserializerTest extends BaseTest[Any] {

  override def env: UManaged[Any] = Managed.unit

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

}
