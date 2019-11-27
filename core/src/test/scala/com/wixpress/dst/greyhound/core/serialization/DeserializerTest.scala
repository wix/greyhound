package com.wixpress.dst.greyhound.core.serialization

import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.specs2.mutable.SpecificationWithJUnit
import zio.{DefaultRuntime, ZIO}

class DeserializerTest
  extends SpecificationWithJUnit
    with DefaultRuntime {

  val topic = "topic"
  val headers = new RecordHeaders

  "map" should {
    "transform the deserialized value" in {
      val intSerializer = new IntegerSerializer
      val intDeserializer = Deserializer(new IntegerDeserializer)
      val stringDeserializer = intDeserializer.map(_.toString)
      val serialized = intSerializer.serialize(topic, 42)

      unsafeRun(stringDeserializer.deserialize(topic, headers, serialized)) must equalTo("42")
    }
  }

  "zip" should {
    "combine 2 deserialized values into a tuple" in {
      val stringSerializer = new StringSerializer
      val stringDeserializer = Deserializer(new StringDeserializer)
      val topicDeserializer = Deserializer((topic, _, _) => ZIO.succeed(topic))
      val topicAndValue = topicDeserializer zip stringDeserializer
      val serialized = stringSerializer.serialize(topic, "foo")

      unsafeRun(topicAndValue.deserialize(topic, headers, serialized)) must equalTo((topic, "foo"))
    }
  }

}
