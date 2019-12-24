package com.wixpress.dst.greyhound.core

import java.nio.charset.StandardCharsets.UTF_8

import com.wixpress.dst.greyhound.core.Headers.Header
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.{Header => KafkaHeader, Headers => KafkaHeaders}
import zio.{Chunk, Task, ZIO}

import scala.collection.JavaConverters._

case class Record[+K, +V](topic: TopicName,
                          partition: Partition,
                          offset: Offset,
                          headers: Headers,
                          key: Option[K],
                          value: V) {

  def id: String = s"$topic:$partition:$offset"

  def bimap[K2, V2](fk: K => K2, fv: V => V2): Record[K2, V2] =
    Record(
      topic = topic,
      partition = partition,
      offset = offset,
      headers = headers,
      key = key.map(fk),
      value = fv(value))

  def mapKey[K2](f: K => K2): Record[K2, V] = bimap(f, identity)

  def mapValue[V2](f: V => V2): Record[K, V2] = bimap(identity, f)

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

case class Headers(headers: Map[Header, Chunk[Byte]] = Map.empty) {
  def +(header: KafkaHeader): Headers =
    copy(headers = headers + (header.key -> Chunk.fromArray(header.value)))

  def +(header: (String, Chunk[Byte])): Headers =
    copy(headers = headers + header)

  def get[A](header: Header, deserializer: Deserializer[A]): Task[Option[A]] =
    ZIO.foreach(headers.get(header))(deserializer.deserialize("", this, _)).map(_.headOption)
}

object Headers {
  type Header = String

  val Empty: Headers = Headers()

  def from(headers: Map[Header, String]): Headers =
    Headers(headers.mapValues(value => Chunk.fromArray(value.getBytes(UTF_8))))

  def from(headers: (Header, String)*): Headers =
    from(headers.toMap)

  def apply(headers: (Header, Chunk[Byte])*): Headers =
    Headers(headers.toMap)

  def apply(headers: KafkaHeaders): Headers =
    headers.asScala.foldLeft(Empty)(_ + _)
}
