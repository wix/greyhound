package com.wixpress.dst.greyhound.core

import org.apache.kafka.clients.consumer.ConsumerRecord

case class Record[+K, +V](topic: String,
                          partition: Int,
                          offset: Long,
                          headers: Headers,
                          key: Option[K],
                          value: V)

object Record {
  def apply[K, V](record: ConsumerRecord[K, V]): Record[K, V] =
    Record(
      topic = record.topic,
      partition = record.partition,
      offset = record.offset,
      headers = Headers(),
      key = Option(record.key),
      value = record.value)
}

case class Headers()

object Headers {
  val Empty: Headers = Headers()
}
