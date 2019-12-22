package com.wixpress.dst.greyhound.core

import org.apache.kafka.common.serialization.{Serde => KafkaSerde}
import zio.{Chunk, Task}

trait Serde[A] extends Serializer[A] with Deserializer[A]

object Serde {
  def apply[A](serde: KafkaSerde[A]): Serde[A] =
    apply(Serializer(serde.serializer()), Deserializer(serde.deserializer()))

  def apply[A](serializer: Serializer[A], deserializer: Deserializer[A]): Serde[A] = new Serde[A] {
    override def deserialize(topic: TopicName, headers: Headers, data: Chunk[Byte]): Task[A] =
      deserializer.deserialize(topic, headers, data)

    override def serialize(topic: String, value: A): Task[Chunk[Byte]] =
      serializer.serialize(topic, value)
  }
}
