package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core.{Offset, Partition, Topic}
import org.apache.kafka.clients.producer.{RecordMetadata => KafkaRecordMetadata}

case class RecordMetadata(topic: Topic, partition: Partition, offset: Offset)

object RecordMetadata {
  def apply(metadata: KafkaRecordMetadata): RecordMetadata =
    RecordMetadata(metadata.topic, metadata.partition, metadata.offset)
}
