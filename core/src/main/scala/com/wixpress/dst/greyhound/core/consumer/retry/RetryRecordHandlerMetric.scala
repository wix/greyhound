package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.{Offset, Partition, Topic, TopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import zio.duration.Duration

sealed trait RetryRecordHandlerMetric extends GreyhoundMetric
sealed trait InterruptibleRetryMetric extends GreyhoundMetric {
  val interrupted: Boolean
}

object RetryRecordHandlerMetric {

  case class BlockingFor(partition: TopicPartition, offset: Long)                                         extends RetryRecordHandlerMetric
  case class BlockingIgnoredForAllFor(partition: TopicPartition, offset: Long)                            extends RetryRecordHandlerMetric
  case class BlockingIgnoredOnceFor(partition: TopicPartition, offset: Long)                              extends RetryRecordHandlerMetric
  case class BlockingRetryHandlerInvocationFailed(partition: TopicPartition, offset: Long, cause: String) extends RetryRecordHandlerMetric
  case class NoRetryOnNonRetryableFailure(partition: TopicPartition, offset: Long, cause: Exception)      extends RetryRecordHandlerMetric
  case object Silent                                                                                      extends RetryRecordHandlerMetric

  case class WaitingBeforeRetry(retryTopic: Topic, retryAttempt: RetryAttempt) extends RetryRecordHandlerMetric

  case class DoneWaitingBeforeRetry(
    retryTopic: Topic,
    partition: Partition,
    offset: Offset,
    retryAttempt: RetryAttempt,
    waitedFor: Duration,
    interrupted: Boolean = false
  ) extends RetryRecordHandlerMetric
      with InterruptibleRetryMetric

  case class RetryProduceFailedWillRetry(
    retryTopic: Topic,
    retryAttempt: Option[RetryAttempt],
    willRetryAfterMs: Long,
    record: ConsumerRecord[_, _],
    error: Throwable
  ) extends RetryRecordHandlerMetric

  case class DoneBlockingBeforeRetry(topic: Topic, partition: Partition, offset: Offset, waitedFor: Duration, interrupted: Boolean = false)
      extends RetryRecordHandlerMetric
      with InterruptibleRetryMetric

}
