package com.wixpress.dst.greyhound.core.consumer.domain

import com.wixpress.dst.greyhound.core.{Partition, Topic}
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}

case class TopicPartition(topic: Topic, partition: Partition) {
  def asKafka: KafkaTopicPartition = new KafkaTopicPartition(topic, partition)
}

object TopicPartition {
  def apply(record: ConsumerRecord[_, _]): TopicPartition =
    TopicPartition(record.topic, record.partition)

  def apply(topicPartition: KafkaTopicPartition): TopicPartition =
    TopicPartition(topicPartition.topic, topicPartition.partition)
}
