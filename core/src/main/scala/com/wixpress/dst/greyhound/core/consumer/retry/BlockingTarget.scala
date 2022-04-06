package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.{Topic, TopicPartition}

sealed trait BlockingTarget {
  def topic: Topic
}

case class TopicTarget(topic: Topic)                            extends BlockingTarget
case class TopicPartitionTarget(topicPartition: TopicPartition) extends BlockingTarget {
  def topic = topicPartition.topic
}
