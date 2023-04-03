package com.wixpress.dst.greyhound.core.consumer.retry

import java.time.{Duration => JavaDuration, Instant}
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.regex.Pattern
import com.wixpress.dst.greyhound.core.Serdes.StringSerde
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryAttempt.{currentTime, RetryAttemptNumber}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.WaitingForRetry
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.{durationDeserializer, instantDeserializer, Group, Headers, Topic}
import zio.Clock
import zio.Duration
import zio.Schedule.spaced
import zio.{Chunk, UIO, URIO, _}

import scala.util.Try

trait NonBlockingRetryHelper {
  def retryTopicsFor(originalTopic: Topic): Set[Topic]

  def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription)(
    implicit trace: Trace
  ): UIO[Option[RetryAttempt]]

  def retryDecision[E](
    retryAttempt: Option[RetryAttempt],
    record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
    error: E,
    subscription: ConsumerSubscription
  )(implicit trace: Trace): URIO[Any, RetryDecision]

  def retrySteps = retryTopicsFor("").size
}

object NonBlockingRetryHelper {
  def apply(group: Group, retryConfig: Option[RetryConfig]): NonBlockingRetryHelper =
    new NonBlockingRetryHelper {

      def policy(topic: Topic): NonBlockingBackoffPolicy =
        retryConfig
          .map(_.nonBlockingBackoffs(originalTopic(topic, group)))
          .getOrElse(NonBlockingBackoffPolicy.empty)

      override def retryTopicsFor(topic: Topic): Set[Topic] =
        policy(topic).intervals.indices.foldLeft(Set.empty[String])((acc, attempt) => acc + s"$topic-$group-retry-$attempt")

      override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription)(
        implicit trace: Trace
      ): UIO[Option[RetryAttempt]] = {
        (for {
          submitted     <- headers.get(RetryHeader.Submitted, instantDeserializer)
          backoff       <- headers.get(RetryHeader.Backoff, durationDeserializer)
          originalTopic <- headers.get[String](RetryHeader.OriginalTopic, StringSerde)
        } yield for {
          ta                                  <- topicAttempt(subscription, topic, originalTopic)
          TopicAttempt(originalTopic, attempt) = ta
          s                                   <- submitted
          b                                   <- backoff
        } yield RetryAttempt(originalTopic, attempt, s, b))
          .catchAll(_ => ZIO.none)
      }

      private def topicAttempt(
        subscription: ConsumerSubscription,
        topic: Topic,
        originalTopicHeader: Option[String]
      ) =
        subscription match {
          case _: Topics       => extractTopicAttempt(group, topic)
          case _: TopicPattern =>
            extractTopicAttemptFromPatternRetryTopic(group, topic, originalTopicHeader)
        }

      override def retryDecision[E](
        retryAttempt: Option[RetryAttempt],
        record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
        error: E,
        subscription: ConsumerSubscription
      )(implicit trace: Trace): URIO[Any, RetryDecision] = currentTime.map(now => {
        val nextRetryAttempt = retryAttempt.fold(0)(_.attempt + 1)
        val originalTopic    = retryAttempt.fold(record.topic)(_.originalTopic)
        val retryTopic       = subscription match {
          case _: TopicPattern => patternRetryTopic(group, nextRetryAttempt)
          case _: Topics       => fixedRetryTopic(originalTopic, group, nextRetryAttempt)
        }

        val topicRetryPolicy = policy(record.topic)
        topicRetryPolicy.intervals
          .lift(nextRetryAttempt)
          .map { backoff =>
            topicRetryPolicy.recordMutate(
              ProducerRecord(
                topic = retryTopic,
                value = record.value,
                key = record.key,
                partition = None,
                headers = record.headers +
                  (RetryHeader.Submitted     -> toChunk(now.toEpochMilli)) +
                  (RetryHeader.Backoff       -> toChunk(backoff.toMillis)) +
                  (RetryHeader.OriginalTopic -> toChunk(originalTopic)) +
                  (RetryHeader.RetryAttempt  -> toChunk(nextRetryAttempt))
              )
            )
          }
          .fold[RetryDecision](NoMoreRetries)(RetryWith)
      })

      private def toChunk(long: Long): Chunk[Byte] =
        Chunk.fromArray(long.toString.getBytes)

      private def toChunk(str: String): Chunk[Byte] =
        Chunk.fromArray(str.getBytes)
    }

  private def extractTopicAttempt[E](group: Group, inputTopic: Topic) =
    inputTopic.split(s"-$group-retry-").toSeq match {
      case Seq(topic, attempt) if Try(attempt.toInt).isSuccess =>
        Some(TopicAttempt(topic, attempt.toInt))
      case _                                                   => None
    }

  private def extractTopicAttemptFromPatternRetryTopic[E](
    group: Group,
    inputTopic: Topic,
    originalTopicHeader: Option[String]
  ) = {
    originalTopicHeader.flatMap(originalTopic => {
      inputTopic.split(s"__gh_pattern-retry-$group-attempt-").toSeq match {
        case Seq(_, attempt) if Try(attempt.toInt).isSuccess =>
          Some(TopicAttempt(originalTopic, attempt.toInt))
        case _                                               => None
      }
    })
  }

  def retryPattern(group: Group) =
    s"__gh_pattern-retry-$group-attempt-\\d\\d*"

  def patternRetryTopic(group: Group, step: Int) =
    s"${patternRetryTopicPrefix(group)}$step"

  def patternRetryTopicPrefix(group: Group) =
    s"__gh_pattern-retry-$group-attempt-"

  def fixedRetryTopic(originalTopic: Topic, group: Group, nextRetryAttempt: Int) =
    s"${fixedRetryTopicPrefix(originalTopic, group)}$nextRetryAttempt"

  def fixedRetryTopicPrefix(originalTopic: Topic, group: Group) =
    s"$originalTopic-$group-retry-"

  def originalTopic(topic: Topic, group: Group): String = {
    val pattern = ("^(.+)-" + Pattern.quote(group) + "-retry-\\d+$").r
    topic match {
      case pattern(originalTopic) => originalTopic
      case _                      => topic
    }
  }
}

object DelayHeaders {
  val Backoff = "backOffTimeMs"
}

object RetryHeader {
  val Submitted     = "submitTimestamp"
  val Backoff       = DelayHeaders.Backoff
  val OriginalTopic = "GH_OriginalTopic"
  val RetryAttempt  = "GH_RetryAttempt"
}

case class RetryAttempt(
  originalTopic: Topic,
  attempt: RetryAttemptNumber,
  submittedAt: Instant,
  backoff: Duration
) {

  def sleep(implicit trace: Trace): URIO[GreyhoundMetrics, Unit] =
    RetryUtil.sleep(submittedAt, backoff) race reportWaitingInIntervals(every = 60.seconds)

  private def reportWaitingInIntervals(every: Duration) =
    report(WaitingForRetry(originalTopic, attempt, submittedAt.toEpochMilli, backoff.toMillis))
      .repeat(spaced(every))
      .unit
}

object RetryUtil {
  def sleep(submittedAt: Instant, backoff: Duration)(implicit trace: Trace): URIO[Any, Unit] = {
    val expiresAt = submittedAt.plus(backoff.asJava)
    currentTime
      .map(_.isAfter(expiresAt))
      .flatMap(expired =>
        if (expired) ZIO.unit
        else
          ZIO.sleep(1.seconds).repeatUntilZIO(_ => currentTime.map(_.isAfter(expiresAt))).unit
      )
  }
}

private case class TopicAttempt(originalTopic: Topic, attempt: Int)

object RetryAttempt {
  type RetryAttemptNumber = Int
  val currentTime = Clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)
}

sealed trait RetryDecision

object RetryDecision {

  case object NoMoreRetries extends RetryDecision

  case class RetryWith(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends RetryDecision

}
