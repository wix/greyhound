package com.wixpress.dst.greyhound.core.serialization

import com.wixpress.dst.greyhound.core.Headers
import org.apache.kafka.common.serialization.{Deserializer => KafkaDeserializer}
import zio.Task

trait Deserializer[+A] {
  def deserialize(topic: String, headers: Headers, data: Array[Byte]): Task[A]

  def map[B](f: A => B): Deserializer[B] =
    (topic: String, headers: Headers, data: Array[Byte]) =>
      deserialize(topic, headers, data).map(f)

  def zip[B](other: Deserializer[B]): Deserializer[(A, B)] =
    (topic: String, headers: Headers, data: Array[Byte]) =>
      deserialize(topic, headers, data) zip other.deserialize(topic, headers, data)
}

object Deserializer {
  def apply[A](deserializer: KafkaDeserializer[A]): Deserializer[A] =
    (topic: String, _: Headers, data: Array[Byte]) =>
      Task(deserializer.deserialize(topic, data))

  def apply[A](f: (String, Headers, Array[Byte]) => Task[A]): Deserializer[A] =
    (topic: String, headers: Headers, data: Array[Byte]) =>
      f(topic, headers, data)
}
