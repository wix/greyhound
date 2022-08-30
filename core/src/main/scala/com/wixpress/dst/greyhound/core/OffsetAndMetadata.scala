package com.wixpress.dst.greyhound.core

import org.apache.kafka.clients.consumer.{OffsetAndMetadata => KafkaOffsetAndMetadata}

case class OffsetAndMetadata(offset: Offset, metadata: Metadata) {
  def asKafka: KafkaOffsetAndMetadata = new KafkaOffsetAndMetadata(offset, metadata)
}

object OffsetAndMetadata {
  def apply(offsetAndMetadata: KafkaOffsetAndMetadata): OffsetAndMetadata =
    OffsetAndMetadata(offsetAndMetadata.offset(), offsetAndMetadata.metadata())

  val NO_METADATA = ""
}
