package com.wixpress.dst.greyhound.core.testkit

import java.lang.Long
import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.retry.{RetryAttempt, RetryPolicy}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.FakeRetryPolicy._
import org.apache.kafka.common.serialization.Serdes.{IntegerSerde, LongSerde}
import zio.clock.Clock
import zio.duration._
import zio.{Chunk, URIO, ZIO, clock}

case class FakeRetryPolicy(topic: TopicName)
  extends RetryPolicy[Clock, HandlerError] {

  override def retryTopics(originalTopic: TopicName): Set[TopicName] =
    Set(originalTopic, s"$originalTopic-retry")

  override def retryAttempt(topic: TopicName, headers: Headers): URIO[Clock, Option[RetryAttempt]] =
    (for {
      attempt <- headers.get(Header.Attempt, intSerde)
      submittedAt <- headers.get(Header.SubmittedAt, instantSerde)
      backoff <- headers.get(Header.Backoff, durationSerde)
    } yield retryAttempt(attempt, submittedAt, backoff)).orElse(ZIO.none)

  override def retryRecord(retryAttempt: Option[RetryAttempt],
                           record: Record[Chunk[Byte], Chunk[Byte]],
                           error: HandlerError): URIO[Clock, Option[ProducerRecord[Chunk[Byte], Chunk[Byte]]]] =
    error match {
      case RetriableError =>
        recordFrom(retryAttempt, record)
          .fold(_ => None, Some(_))

      case NonRetriableError => ZIO.none
    }

  private def retryAttempt(attempt: Option[Int],
                           submittedAt: Option[Instant],
                           backoff: Option[Duration]) =
    for {
      a <- attempt
      s <- submittedAt
      b <- backoff
    } yield RetryAttempt(a, s, b)

  private def recordFrom(retryAttempt: Option[RetryAttempt], record: Record[Chunk[Byte], Chunk[Byte]]) =
    for {
      now <- currentTime
      nextRetryAttempt = retryAttempt.fold(0)(_.attempt + 1)
      retryAttempt <- intSerde.serialize(topic, nextRetryAttempt)
      submittedAt <- instantSerde.serialize(topic, now)
      backoff <- durationSerde.serialize(topic, 1.second)
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

object FakeRetryPolicy {
  object Header {
    val Attempt = "retry-attempt"
    val SubmittedAt = "retry-submitted-at"
    val Backoff = "retry-backoff"
  }

  val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)
  val intSerde = Serde(new IntegerSerde).inmap(_.toInt)(Integer.valueOf)
  val longSerde = Serde(new LongSerde).inmap(_.toLong)(Long.valueOf)
  val instantSerde = longSerde.inmap(Instant.ofEpochMilli)(_.toEpochMilli)
  val durationSerde = longSerde.inmap(Duration.fromNanos)(_.toNanos)
}

sealed trait HandlerError
case object RetriableError extends HandlerError
case object NonRetriableError extends HandlerError
