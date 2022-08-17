package com.wixpress.dst.greyhound.core.serialization

import com.wixpress.dst.greyhound.core.testkit.{BaseTest, BaseTestNoEnv}
import com.wixpress.dst.greyhound.core.{Deserializer, Headers, Serializer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer}

case class IntAndString(i: Int, s: String)

class DeserializerTest extends BaseTestNoEnv {

  val topic   = "topic"
  val headers = Headers.Empty

  "map" should {
    "transform the deserialized value" in {
      val intSerializer      = Serializer(new IntegerSerializer)
      val intDeserializer    = Deserializer(new IntegerDeserializer)
      val stringDeserializer = intDeserializer.map(_.toString)

      for {
        serialized   <- intSerializer.serialize(topic, 42)
        deserialized <- stringDeserializer.deserialize(topic, headers, serialized)
      } yield deserialized must equalTo("42")
    }
  }

}
