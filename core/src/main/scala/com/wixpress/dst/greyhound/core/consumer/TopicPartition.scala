package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.{Partition, Topic}

case class TopicPartition(topic: Topic, partition: Partition)

object TopicPartition {
  def apply(record: ConsumerRecord[_, _]): TopicPartition =
    TopicPartition(record.topic, record.partition)
}
