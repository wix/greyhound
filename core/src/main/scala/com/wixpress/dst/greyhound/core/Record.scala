package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.Headers.Header
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.{Header => KafkaHeader, Headers => KafkaHeaders}
import zio.{Task, ZIO}

import scala.collection.JavaConverters._

case class Record[+K, +V](topic: TopicName,
                          partition: Partition,
                          offset: Offset,
                          headers: Headers,
                          key: Option[K],
                          value: V) {

  def id: String = s"$topic:$partition:$offset"

}

object Record {
  def apply[K, V](record: ConsumerRecord[K, V]): Record[K, V] =
    Record(
      topic = record.topic,
      partition = record.partition,
      offset = record.offset,
      headers = Headers(record.headers),
      key = Option(record.key),
      value = record.value)
}

case class Headers(headers: Map[Header, Array[Byte]] = Map.empty) {
  def +(header: KafkaHeader): Headers =
    copy(headers = headers + (header.key -> header.value))

  def get[A](header: Header, deserializer: Deserializer[A]): Task[Option[A]] =
    ZIO.foreach(headers.get(header))(deserializer.deserialize("", this, _)).map(_.headOption)
}

object Headers {
  type Header = String

  val Empty: Headers = Headers()

  def apply(headers: (Header, Array[Byte])*): Headers =
    Headers(headers.toMap)

  def apply(headers: KafkaHeaders): Headers =
    headers.asScala.foldLeft(Empty)(_ + _)
}
