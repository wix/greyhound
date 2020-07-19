package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.{Headers, Offset}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{BlockingFor, _}

sealed trait BlockingState {
  def metric[V, K](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric
}

object BlockingState {
  case object Normal extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]) = Silent
  }

  case class Blocked[V, K](key: Option[K], value: V, messageHeaders: Headers, topicPartition: TopicPartition, offset: Offset) extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]) =
      BlockingFor(TopicPartition(record.topic, record.partition), record.offset)
  }

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

  def shouldBlockFrom[V, K](blockingState: BlockingState) = {
    blockingState match {
      case Blocking => true
      case _: Blocked[V, K] => true
      case IgnoringAll => false
      case IgnoringOnce => false
    }
  }
}