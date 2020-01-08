package com.wixpress.dst.greyhound.core

import java.nio.charset.StandardCharsets.UTF_8

import com.wixpress.dst.greyhound.core.Headers.Header
import org.apache.kafka.common.header.{Header => KafkaHeader, Headers => KafkaHeaders}
import zio.{Chunk, Task, ZIO}

import scala.collection.JavaConverters._

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
