package com.wixpress.dst.greyhound.core.testkit

import java.time.Instant
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.retry.{BlockingHandlerFailed, NonBlockingRetryHelper, RetryAttempt, RetryDecision, RetryHeader}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.FakeRetryHelper._
import zio._
import zio.Clock

trait FakeNonBlockingRetryHelper extends NonBlockingRetryHelper {
  val topic: Topic

  override def retryTopicsFor(originalTopic: Topic): Set[Topic] =
    Set(s"$originalTopic-retry")

  override def retryDecision[E](
    retryAttempt: Option[RetryAttempt],
    record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
    error: E,
    subscription: ConsumerSubscription
  )(implicit trace: Trace): URIO[Any, RetryDecision] =
    error match {
      case RetriableError | BlockingHandlerFailed =>
        currentTime.map(now =>
          RetryWith(recordFrom(now, retryAttempt, record))
        )
      case NonRetriableError                      =>
        ZIO.succeed(NoMoreRetries)
    }

  private def recordFrom(now: Instant, retryAttempt: Option[RetryAttempt], record: ConsumerRecord[Chunk[Byte], Chunk[Byte]])(
    implicit trace: Trace
  ) = {
    val nextRetryAttempt = retryAttempt.fold(0)(_.attempt + 1)
    ProducerRecord(
      topic = s"$topic-retry",
      value = record.value,
      key = record.key,
      partition = None,
      headers = RetryAttempt.toHeaders(RetryAttempt(topic, nextRetryAttempt, now, 1.second))
    )
  }
}

case class FakeRetryHelper(topic: Topic) extends FakeNonBlockingRetryHelper

object FakeRetryHelper {
  implicit private val trace = Trace.empty

  val currentTime: UIO[Instant] = Clock.instant
}

sealed trait HandlerError

case object RetriableError extends HandlerError

case object NonRetriableError extends HandlerError
