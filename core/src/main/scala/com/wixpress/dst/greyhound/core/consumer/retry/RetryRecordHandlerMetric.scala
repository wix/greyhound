package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.{Topic, TopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import zio.duration.Duration

import java.time.Instant

sealed trait RetryRecordHandlerMetric extends GreyhoundMetric

object RetryRecordHandlerMetric {

  case class BlockingFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingIgnoredForAllFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingIgnoredOnceFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingRetryHandlerInvocationFailed(partition: TopicPartition, offset: Long, cause: String) extends RetryRecordHandlerMetric
  case class NoRetryOnNonRetryableFailure(partition: TopicPartition, offset: Long, cause: Exception) extends RetryRecordHandlerMetric
  case object Silent extends RetryRecordHandlerMetric

  case class WaitingBeforeRetry(retryTopic: Topic,
                                retryAttempt: RetryAttempt) extends RetryRecordHandlerMetric

  case class DoneWaitingBeforeRetry(retryTopic: Topic,
                                    retryAttempt: RetryAttempt,
                                    waitedFor: Duration,
                                    interrupted: Boolean = false
                                   ) extends RetryRecordHandlerMetric

  case class RetryProduceFailedWillRetry(retryTopic: Topic,
                                         retryAttempt: Option[RetryAttempt],
                                         willRetryAfterMs: Long,
                                         error: Throwable) extends RetryRecordHandlerMetric

}
