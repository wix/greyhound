package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import org.apache.kafka.clients.consumer.{ConsumerRecord => KafkaConsumerRecord}

case class ConsumerRecord[+K, +V](topic: Topic,
                                  partition: Partition,
                                  offset: Offset,
                                  headers: Headers,
                                  key: Option[K],
                                  value: V) {

  def id: String = s"$topic:$partition:$offset"

  def bimap[K2, V2](fk: K => K2, fv: V => V2): ConsumerRecord[K2, V2] =
    ConsumerRecord(
      topic = topic,
      partition = partition,
      offset = offset,
      headers = headers,
      key = key.map(fk),
      value = fv(value))

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
      value = record.value)
}
