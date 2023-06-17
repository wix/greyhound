package com.wixpress.dst.greyhound.core

import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}

case class TopicPartition(topic: Topic, partition: Partition) {
  def asKafka: KafkaTopicPartition = new KafkaTopicPartition(topic, partition)
}

object TopicPartition {
  def fromKafka(topicPartition: KafkaTopicPartition): TopicPartition =
    TopicPartition(topicPartition.topic, topicPartition.partition)
  def apply(topicPartition: KafkaTopicPartition): TopicPartition =
    TopicPartition(topicPartition.topic, topicPartition.partition)
}

case class TopicPartitionOffset(topic: Topic, partition: Partition, offset: Offset)
