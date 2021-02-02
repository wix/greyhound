package com.wixpress.dst.greyhound.core.consumer.domain

import com.wixpress.dst.greyhound.core.TopicPartition

object RecordTopicPartition {
  def apply(record: ConsumerRecord[_, _]): TopicPartition =
    TopicPartition(record.topic, record.partition)
}
