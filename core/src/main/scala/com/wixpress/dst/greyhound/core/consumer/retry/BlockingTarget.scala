package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.consumer.domain.TopicPartition

sealed trait BlockingTarget

case class TopicTarget(topic: Topic) extends BlockingTarget
case class TopicPartitionTarget(topicPartition: TopicPartition) extends BlockingTarget