package com.wixpress.dst.greyhound.core.consumer

import java.time.{Instant, Duration => JavaDuration}
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RetryAttempt.{RetryAttemptNumber, currentTime}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import zio._
import zio.clock.Clock
import zio.duration.Duration

trait RetryPolicy {
  def retryTopicsFor(originalTopic: Topic): Set[Topic]

  def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]]

  def retryDecision[E](retryAttempt: Option[RetryAttempt],
                       record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
                       error: E,
                       subscription: ConsumerSubscription): URIO[Clock, RetryDecision]

  def retrySteps = retryTopicsFor("").size

  def intervals: Seq[Duration]

}

object RetryPolicy {
  def blockingOnly(backoffs: Duration*): RetryPolicy =
    new RetryPolicy {
      override def intervals: Seq[Duration] = backoffs.toSeq

      override def retryTopicsFor(originalTopic: Topic): Set[Topic] = Set(originalTopic)

      override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]] = UIO(None)

      override def retryDecision[E](retryAttempt: Option[RetryAttempt], record: ConsumerRecord[Chunk[Byte], Chunk[Byte]], error: E, subscription: ConsumerSubscription): URIO[Clock, RetryDecision] = ???
    }

  def nonBlockingOnly(group: Group, backoffs: Duration*): RetryPolicy = NonBlockingRetryPolicy.defaultNonBlocking(group, backoffs: _*)
}

private case class TopicAttempt(originalTopic: Topic, attempt: Int)

case class NonRetryableException(cause: Exception) extends Exception(cause)

object RetryHeader {
  val Submitted = "submitTimestamp"
  val Backoff = "backOffTimeMs"
  val OriginalTopic = "GH_OriginalTopic"
}

case class RetryAttempt(originalTopic: Topic,
                        attempt: RetryAttemptNumber,
                        submittedAt: Instant,
                        backoff: Duration) {

  def sleep: URIO[Clock, Unit] = currentTime.flatMap { now =>
    val expiresAt = submittedAt.plus(backoff.asJava)
    val sleep = JavaDuration.between(now, expiresAt)
    clock.sleep(Duration.fromJava(sleep))
  }

}

object RetryAttempt {
  type RetryAttemptNumber = Int

  val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)
}

sealed trait RetryDecision

object RetryDecision {

  case object NoMoreRetries extends RetryDecision

  case class RetryWith(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends RetryDecision

}

