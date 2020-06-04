package com.wixpress.dst.greyhound.core.testkit

import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.{ConsumerRecord, ConsumerSubscription, RetryAttempt, RetryDecision, RetryPolicy}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.FakeRetryPolicy._
import zio.clock.Clock
import zio.duration._
import zio.{Chunk, Runtime, UIO, URIO, ZIO, clock}

case class FakeRetryPolicy(topic: Topic)
  extends RetryPolicy {

  override def retryTopicsFor(originalTopic: Topic): Set[Topic] =
    Set(s"$originalTopic-retry")

  override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]] =
    (for {
      attempt <- headers.get(Header.Attempt, IntSerde)
      submittedAt <- headers.get(Header.SubmittedAt, InstantSerde)
      backoff <- headers.get(Header.Backoff, DurationSerde)
    } yield retryAttemptInternal(topic, attempt, submittedAt, backoff)).orElse(ZIO.none)

  override def retryDecision[E](retryAttempt: Option[RetryAttempt],
                                record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
                                error: E,
                                subscription: ConsumerSubscription): URIO[Clock, RetryDecision] =
    error match {
      case RetriableError =>
        currentTime.flatMap(now => recordFrom(now, retryAttempt, record)
          .fold(_ => NoMoreRetries, RetryWith))
      case NonRetriableError =>
        UIO(NoMoreRetries)
    }

  private def retryAttemptInternal(topic: Topic,
                           attempt: Option[Int],
                           submittedAt: Option[Instant],
                           backoff: Option[Duration]) =
    for {
      a <- attempt
      s <- submittedAt
      b <- backoff
    } yield RetryAttempt(topic, a, s, b)

  private def recordFrom(now: Instant, retryAttempt: Option[RetryAttempt], record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]) = {
    val nextRetryAttempt = retryAttempt.fold(0)(_.attempt + 1)
    for {
      retryAttempt <- IntSerde.serialize(topic, nextRetryAttempt)
      submittedAt <- InstantSerde.serialize(topic, now)
      backoff <- DurationSerde.serialize(topic, 1.second)
    } yield ProducerRecord(
      topic = s"$topic-retry",
      value = record.value,
      key = record.key,
      partition = None,
      headers = Headers(
        Header.Attempt -> retryAttempt,
        Header.SubmittedAt -> submittedAt,
        Header.Backoff -> backoff))
  }
}

object FakeRetryPolicy {

  object Header {
    val Attempt = "retry-attempt"
    val SubmittedAt = "retry-submitted-at"
    val Backoff = "retry-backoff"
  }

  val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)
}

sealed trait HandlerError

case object RetriableError extends HandlerError

case object NonRetriableError extends HandlerError
