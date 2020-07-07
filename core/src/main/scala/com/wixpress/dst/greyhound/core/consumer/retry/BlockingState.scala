package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{BlockingFor, _}

sealed trait BlockingState {
  def metric[V, K](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric
}

object BlockingState {
  case object Blocking extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]) =
      BlockingFor(TopicPartition(record.topic, record.partition), record.offset)
  }
  case object IgnoringAll extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric =
      BlockingIgnoredForAllFor(TopicPartition(record.topic, record.partition), record.offset)
  }
  case object IgnoringOnce extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric =
      BlockingIgnoredOnceFor(TopicPartition(record.topic, record.partition), record.offset)
  }

  def shouldBlockFrom(blockingState: BlockingState) = {
    blockingState match {
      case Blocking => true
      case IgnoringAll => false
      case IgnoringOnce => false
    }
  }
}