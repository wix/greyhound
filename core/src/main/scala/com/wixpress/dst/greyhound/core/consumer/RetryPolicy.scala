package com.wixpress.dst.greyhound.core.consumer

import java.time.{Instant, Duration => JavaDuration}
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RetryAttempt.{RetryAttemptNumber, currentTime}
import com.wixpress.dst.greyhound.core.consumer.RetryDecision.NoMoreRetries
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

  def blockingRetries: BlockingRetries

}

sealed trait BlockingRetries {
  def blockingIntervals: Seq[Duration]
}

case object NonBlockingRetries extends BlockingRetries{
  override def blockingIntervals: Seq[Duration] = Seq.empty
}

case class BlockingRetriesFollowedByNonBlocking(_blockingIntervals: Seq[Duration]) extends BlockingRetries{
  override def blockingIntervals: Seq[Duration] = _blockingIntervals
}

case class FiniteBlockingRetriesOnly(_blockingIntervals: Seq[Duration]) extends BlockingRetries{
  override def blockingIntervals: Seq[Duration] = _blockingIntervals
}

case class InfiniteBlockingRetriesOnly(interval: Duration) extends BlockingRetries{
  override def blockingIntervals: Seq[Duration] =  Stream.continually(interval)
}

object RetryPolicy {
  def blockingFollowedByNonBlocking(group: String, blockingBackoffs: Seq[Duration], nonBlockingBackoffs: Seq[Duration]) =
    BlockingAndNonBlockingRetryPolicy.blockingThenNonBlocking(group, blockingBackoffs, nonBlockingBackoffs)

  def blockingOnly(backoffs: Duration*): RetryPolicy =
    new NoOpNonBlockingRetryPolicy {
      override def blockingRetries: BlockingRetries = FiniteBlockingRetriesOnly(backoffs)
    }

  def infiniteBlocking(interval: Duration): RetryPolicy =
    new NoOpNonBlockingRetryPolicy {
      override def blockingRetries: BlockingRetries = InfiniteBlockingRetriesOnly(interval)
    }

  def nonBlockingOnly(group: Group, backoffs: Duration*): RetryPolicy = NonBlockingRetryPolicy.defaultNonBlocking(group, backoffs: _*)
}

private case class TopicAttempt(originalTopic: Topic, attempt: Int)

case class NonRetryableException(cause: Exception) extends Exception(cause)
case object BlockingHandlerFailed extends RuntimeException

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

trait NoOpNonBlockingRetryPolicy extends RetryPolicy {
  override def retryTopicsFor(originalTopic: Topic): Set[Topic] = Set.empty

  override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]] = UIO(None)

  override def retryDecision[E](retryAttempt: Option[RetryAttempt],
                       record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
                       error: E,
                       subscription: ConsumerSubscription): URIO[Clock, RetryDecision] = UIO(NoMoreRetries)
}