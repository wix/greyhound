package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import org.apache.kafka.clients.consumer.{ConsumerRecord => KafkaConsumerRecord}
import zio.ZIO

case class ConsumerRecord[+K, +V](topic: Topic,
                                  partition: Partition,
                                  offset: Offset,
                                  headers: Headers,
                                  key: Option[K],
                                  value: V,
                                  pollTime: Long,
                                  bytesTotal: Long,
                                  producedTimestamp: Long) {
  def id: String = s"$topic:$partition:$offset"

  def bimap[K2, V2](fk: K => K2, fv: V => V2): ConsumerRecord[K2, V2] =
    ConsumerRecord(
      topic = topic,
      partition = partition,
      offset = offset,
      headers = headers,
      key = key.map(fk),
      value = fv(value),
      pollTime = pollTime,
      bytesTotal = bytesTotal,
      producedTimestamp = producedTimestamp)

  def bimapM[R, E, K2, V2](fk: K => ZIO[R, E, K2], fv: V => ZIO[R, E, V2]): ZIO[R, E, ConsumerRecord[K2, V2]] =
    for {
      key2 <- ZIO.foreach(key)(fk)
      value2 <- fv(value)
    } yield ConsumerRecord(
      topic = topic,
      partition = partition,
      offset = offset,
      headers = headers,
      key = key2.headOption,
      value = value2,
      pollTime = pollTime,
      bytesTotal = bytesTotal,
      producedTimestamp = producedTimestamp)

  def mapKey[K2](f: K => K2): ConsumerRecord[K2, V] = bimap(f, identity)

  def mapValue[V2](f: V => V2): ConsumerRecord[K, V2] = bimap(identity, f)

}

object ConsumerRecord {
  def apply[K, V](record: KafkaConsumerRecord[K, V]): ConsumerRecord[K, V] =
    consumer.ConsumerRecord(
      topic = record.topic,
      partition = record.partition,
      offset = record.offset,
      headers = Headers(record.headers),
      key = Option(record.key),
      value = record.value,
      pollTime = System.currentTimeMillis,
      producedTimestamp = record.timestamp,
      bytesTotal = record.serializedValueSize() + record.serializedKeySize() + record.headers().toArray.map(h => h.key.length + h.value.length).sum)
}
