package com.wixpress.dst.greyhound.core

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.{Headers => KafkaHeaders}

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

case class Headers()

object Headers {
  val Empty: Headers = Headers()

  // TODO
  def apply(headers: KafkaHeaders): Headers = Empty
}
