package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Serdes.StringSerde
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper.attemptNumberFromTopic
import com.wixpress.dst.greyhound.core.consumer.retry.RetryAttempt.RetryAttemptNumber
import zio._

import java.time.Instant

/**
 * Description of a retry attempt
 * @param attempt
 *   contains which attempt is it, starting from 0 including blocking and non-blocking attempts
 */
case class RetryAttempt(
  originalTopic: Topic,
  attempt: RetryAttemptNumber,
  submittedAt: Instant,
  backoff: Duration
)

object RetryHeader {
  val Submitted     = "submitTimestamp"
  val Backoff       = DelayHeaders.Backoff
  val OriginalTopic = "GH_OriginalTopic"
  val RetryAttempt  = "GH_RetryAttempt"
}

case class RetryAttemptHeaders(
  originalTopic: Option[Topic],
  attempt: Option[RetryAttemptNumber],
  submittedAt: Option[Instant],
  backoff: Option[Duration]
)

object RetryAttemptHeaders {
  def fromHeaders(headers: Headers): Task[RetryAttemptHeaders] =
    for {
      submitted <- headers.get(RetryHeader.Submitted, instantDeserializer)
      backoff   <- headers.get(RetryHeader.Backoff, durationDeserializer)
      topic     <- headers.get[String](RetryHeader.OriginalTopic, StringSerde)
      attempt   <- headers.get(RetryHeader.RetryAttempt, longDeserializer)
    } yield RetryAttemptHeaders(topic, attempt.map(_.toInt), submitted, backoff)
}

object RetryAttempt {
  type RetryAttemptNumber = Int

  private def toChunk(str: String): Chunk[Byte] = Chunk.fromArray(str.getBytes)

  def toHeaders(attempt: RetryAttempt): Headers = Headers(
    RetryHeader.Submitted     -> toChunk(attempt.submittedAt.toEpochMilli.toString),
    RetryHeader.Backoff       -> toChunk(attempt.backoff.toMillis.toString),
    RetryHeader.OriginalTopic -> toChunk(attempt.originalTopic),
    RetryHeader.RetryAttempt  -> toChunk(attempt.attempt.toString)
  )

  /** @return None on infinite blocking retries */
  def maxBlockingAttempts(topic: Topic, retryConfig: Option[RetryConfig]): Option[Int] =
    retryConfig.map(_.blockingBackoffs(topic)).fold(Option(0)) {
      case finite if finite.hasDefiniteSize => Some(finite.size)
      case _                                => None
    }

  /** @return None on infinite retries */
  def maxOverallAttempts(topic: Topic, retryConfig: Option[RetryConfig]): Option[Int] =
    maxBlockingAttempts(topic, retryConfig).map {
      _ + retryConfig.fold(0)(_.nonBlockingBackoffs(topic).length)
    }

  def extract(
    headers: Headers,
    topic: Topic,
    group: Group,
    subscription: ConsumerSubscription,
    retryConfig: Option[RetryConfig]
  )(implicit trace: Trace): UIO[Option[RetryAttempt]] = {

    def maybeNonBlockingAttempt(hs: RetryAttemptHeaders): Option[RetryAttempt] =
      for {
        submitted                            <- hs.submittedAt
        backoff                              <- hs.backoff
        TopicAttempt(originalTopic, attempt) <- attemptNumberFromTopic(subscription, topic, hs.originalTopic, group)
        blockingRetries                       = maxBlockingAttempts(originalTopic, retryConfig).getOrElse(0)
      } yield RetryAttempt(originalTopic, blockingRetries + attempt, submitted, backoff)

    def maybeBlockingAttempt(hs: RetryAttemptHeaders): Option[RetryAttempt] =
      for {
        submitted     <- hs.submittedAt
        backoff       <- hs.backoff
        originalTopic <- hs.originalTopic if originalTopic == topic
        attempt       <- hs.attempt
      } yield RetryAttempt(originalTopic, attempt, submitted, backoff)

    RetryAttemptHeaders.fromHeaders(headers).map { hs => maybeNonBlockingAttempt(hs) orElse maybeBlockingAttempt(hs) }
  }.catchAll(_ => ZIO.none)
}
