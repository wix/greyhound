package com.wixpress.dst.greyhound.core

import org.apache.kafka.common.{PartitionInfo, TopicPartition => KafkaTopicPartition}

case class TopicPartition(topic: Topic, partition: Partition) {
  def asKafka: KafkaTopicPartition = new KafkaTopicPartition(topic, partition)
}

object TopicPartition {
  def apply(topicPartition: KafkaTopicPartition): TopicPartition =
    TopicPartition(topicPartition.topic, topicPartition.partition)

  def apply(into: PartitionInfo): TopicPartition =
    TopicPartition(into.topic(), into.partition())
}
