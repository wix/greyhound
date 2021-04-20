package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{BlockingFor, BlockingIgnoredForAllFor, BlockingIgnoredOnceFor}
import com.wixpress.dst.greyhound.core.{Headers, Offset, TopicPartition}

sealed trait BlockingState {
  def metric[K, V](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric
}

object BlockingState {
  case class Blocked[K, V](key: Option[K], value: V, messageHeaders: Headers, topicPartition: TopicPartition, offset: Offset) extends BlockingState {
    override def metric[K, V](record: ConsumerRecord[K, V]): BlockingFor =
      BlockingFor(TopicPartition(record.topic, record.partition), record.offset)
  }

  case object Blocking extends BlockingState {
    override def metric[K, V](record: ConsumerRecord[K, V]): BlockingFor =
      BlockingFor(TopicPartition(record.topic, record.partition), record.offset)
  }

  case object IgnoringAll extends BlockingState {
    override def metric[K, V](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric =
      BlockingIgnoredForAllFor(TopicPartition(record.topic, record.partition), record.offset)
  }

  case object IgnoringOnce extends BlockingState {
    override def metric[K, V](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric =
      BlockingIgnoredOnceFor(TopicPartition(record.topic, record.partition), record.offset)
  }

  def shouldBlockFrom(blockingState: BlockingState): Boolean =
    blockingState match {
      case Blocking => true
      case _: Blocked[_, _ ] => true
      case IgnoringAll => false
      case IgnoringOnce => false
    }
}