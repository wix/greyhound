package com.wixpress.dst.greyhound.core.serialization

import com.wixpress.dst.greyhound.core.Headers
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.specs2.mutable.SpecificationWithJUnit
import zio.{DefaultRuntime, ZIO}

class DeserializerTest
  extends SpecificationWithJUnit
    with DefaultRuntime {

  val topic = "topic"
  val headers = Headers.Empty

  "map" should {
    "transform the deserialized value" in unsafeRun {
      val intSerializer = new IntegerSerializer
      val intDeserializer = Deserializer(new IntegerDeserializer)
      val stringDeserializer = intDeserializer.map(_.toString)
      val serialized = intSerializer.serialize(topic, 42)

      stringDeserializer.deserialize(topic, headers, serialized).map(_ must equalTo("42"))
    }
  }

  "zip" should {
    "combine 2 deserialized values into a tuple" in unsafeRun {
      val stringSerializer = new StringSerializer
      val stringDeserializer = Deserializer(new StringDeserializer)
      val topicDeserializer = Deserializer((topic, _, _) => ZIO.succeed(topic))
      val topicAndValue = topicDeserializer zip stringDeserializer
      val serialized = stringSerializer.serialize(topic, "foo")

      topicAndValue.deserialize(topic, headers, serialized).map(_ must equalTo(topic -> "foo"))
    }
  }

}
