package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.TopicPartition
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric

sealed trait RetryRecordHandlerMetric extends GreyhoundMetric

object RetryRecordHandlerMetric {

  case class BlockingFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingIgnoredForAllFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingIgnoredOnceFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingRetryOnHandlerFailed(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric

}