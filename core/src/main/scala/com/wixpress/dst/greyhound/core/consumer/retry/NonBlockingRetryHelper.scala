package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.WaitingForRetry
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.{Group, Topic}
import zio.Schedule.spaced
import zio.{Chunk, Clock, Duration, URIO, _}

import java.time.Instant
import java.util.regex.Pattern
import scala.util.Try

trait NonBlockingRetryHelper {
  def retryTopicsFor(originalTopic: Topic): Set[Topic]

  def retryDecision[E](
    retryAttempt: Option[RetryAttempt],
    record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
    error: E,
    subscription: ConsumerSubscription
  )(implicit trace: Trace): URIO[Any, RetryDecision]

  def retrySteps: Int = retryTopicsFor("").size
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

      override def retryDecision[E](
        retryAttempt: Option[RetryAttempt],
        record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
        error: E,
        subscription: ConsumerSubscription
      )(implicit trace: Trace): URIO[Any, RetryDecision] = Clock.instant.map(now => {
        val blockingRetriesBefore = RetryAttempt.maxBlockingAttempts(
          NonBlockingRetryHelper.originalTopic(record.topic, group),
          retryConfig
        ).getOrElse(0)

        // attempt if present contains full number of retries
        val nextNonBlockingAttempt = retryAttempt.fold(0)(_.attempt + 1 - blockingRetriesBefore)
        val nextRetryAttempt = nextNonBlockingAttempt + blockingRetriesBefore
        val originalTopic    = retryAttempt.fold(record.topic)(_.originalTopic)
        val retryTopic       = subscription match {
          case _: TopicPattern => patternRetryTopic(group, nextNonBlockingAttempt)
          case _: Topics       => fixedRetryTopic(originalTopic, group, nextNonBlockingAttempt)
        }

        val topicRetryPolicy = policy(record.topic)
        topicRetryPolicy.intervals
          .lift(nextNonBlockingAttempt)
          .map { backoff =>
            val attempt = RetryAttempt(
              attempt = nextRetryAttempt,
              originalTopic = originalTopic,
              submittedAt = now,
              backoff = backoff
            )
            topicRetryPolicy.recordMutate(
              ProducerRecord(
                topic = retryTopic,
                value = record.value,
                key = record.key,
                partition = None,
                headers = record.headers ++ RetryAttempt.toHeaders(attempt)
              )
            )
          }
          .fold[RetryDecision](NoMoreRetries)(RetryWith)
      })
    }

  private[retry] def attemptNumberFromTopic(
    subscription: ConsumerSubscription,
    topic: Topic,
    originalTopicHeader: Option[String],
    group: Group
  ) =
    subscription match {
      case _: Topics => extractTopicAttempt(group, topic)
      case _: TopicPattern =>
        extractTopicAttemptFromPatternRetryTopic(group, topic, originalTopicHeader)
    }

  private def extractTopicAttempt(group: Group, inputTopic: Topic) =
    inputTopic.split(s"-$group-retry-").toSeq match {
      case Seq(topic, attempt) if Try(attempt.toInt).isSuccess =>
        Some(TopicAttempt(topic, attempt.toInt))
      case _ => None
    }

  private def extractTopicAttemptFromPatternRetryTopic(
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

object RetryUtil {
  def sleep(attempt: RetryAttempt)(implicit trace: Trace): URIO[GreyhoundMetrics, Unit] =
    sleep(attempt.submittedAt, attempt.backoff) race
      report(WaitingForRetry(attempt.originalTopic, attempt.attempt, attempt.submittedAt.toEpochMilli, attempt.backoff.toMillis))
        .repeat(spaced(60.seconds))
        .unit

  def sleep(submittedAt: Instant, backoff: Duration)(implicit trace: Trace): URIO[Any, Unit] = {
    val expiresAt = submittedAt.plus(backoff.asJava)
    Clock.instant
      .map(_.isAfter(expiresAt))
      .flatMap(expired =>
        if (expired) ZIO.unit
        else
          ZIO.sleep(1.second).repeatUntilZIO(_ => Clock.instant.map(_.isAfter(expiresAt))).unit
      )
  }
}

private case class TopicAttempt(originalTopic: Topic, attempt: Int)

sealed trait RetryDecision

object RetryDecision {

  case object NoMoreRetries extends RetryDecision

  case class RetryWith(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends RetryDecision

}
