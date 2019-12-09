package com.wixpress.dst.greyhound.core

import org.apache.kafka.common.serialization.{Serializer => KafkaSerializer}
import zio.Task

trait Serializer[-A] {
  def serialize(topic: String, value: A): Task[Array[Byte]]
}

object Serializer {
  def apply[A](serializer: KafkaSerializer[A]): Serializer[A] =
    (topic: String, value: A) => Task(serializer.serialize(topic, value))
}
