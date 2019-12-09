package com.wixpress.dst.greyhound.core

import org.apache.kafka.common.serialization.{Deserializer => KafkaDeserializer}
import zio.Task

trait Deserializer[+A] {
  def deserialize(topic: TopicName, headers: Headers, data: Array[Byte]): Task[A]

  def map[B](f: A => B): Deserializer[B] =
    (topic: TopicName, headers: Headers, data: Array[Byte]) =>
      deserialize(topic, headers, data).map(f)

  def zip[B](other: Deserializer[B]): Deserializer[(A, B)] =
    (topic: TopicName, headers: Headers, data: Array[Byte]) =>
      deserialize(topic, headers, data) zip other.deserialize(topic, headers, data)
}

object Deserializer {
  def apply[A](deserializer: KafkaDeserializer[A]): Deserializer[A] =
    (topic: TopicName, _: Headers, data: Array[Byte]) =>
      Task(deserializer.deserialize(topic, data))

  def apply[A](f: (TopicName, Headers, Array[Byte]) => Task[A]): Deserializer[A] =
    (topic: TopicName, headers: Headers, data: Array[Byte]) =>
      f(topic, headers, data)
}
