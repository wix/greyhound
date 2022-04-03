package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core._

final case class ProducerRecord[+K, +V] private[greyhound] (
  topic: Topic,
  value: Option[V],
  key: Option[K],
  partition: Option[Partition],
  headers: Headers
)

object ProducerRecord {
  def apply[K, V](
    topic: Topic,
    value: V,
    key: Option[K] = None,
    partition: Option[Partition] = None,
    headers: Headers = Headers.Empty
  ): ProducerRecord[K, V] =
    ProducerRecord(topic, Option(value), key, partition, headers)

  def tombstone[K](topic: Topic, key: Option[K] = None, partition: Option[Partition] = None, headers: Headers = Headers.Empty) =
    ProducerRecord[K, Nothing](topic, None, key, partition, headers)
}
