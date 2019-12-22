package com.wixpress.dst.greyhound.core

import org.apache.kafka.common.serialization.{Serializer => KafkaSerializer}
import zio.{Chunk, Task}

trait Serializer[-A] {
  def serialize(topic: String, value: A): Task[Chunk[Byte]]

  def contraMap[B](f: B => A): Serializer[B] =
    (topic: String, value: B) => serialize(topic, f(value))
}

object Serializer {
  def apply[A](serializer: KafkaSerializer[A]): Serializer[A] =
    (topic: String, value: A) => Task(serializer.serialize(topic, value)).map(Chunk.fromArray)
}
