package com.wixpress.dst.greyhound.core.consumer

import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core.Serdes.StringSerde
import com.wixpress.dst.greyhound.core.consumer.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.RetryAttempt.currentTime
import com.wixpress.dst.greyhound.core.consumer.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.{Group, Headers, Topic}
import zio.clock.Clock
import zio.duration.Duration
import zio.{Chunk, UIO, URIO, _}

import scala.util.Try

object NonBlockingRetryPolicy {
  def defaultNonBlocking(group: Group, backoffs: Duration*): RetryPolicy =
      new RetryPolicy {
        private val longDeserializer = StringSerde.mapM(string => Task(string.toLong))

        override def intervals: Seq[Duration] = Seq.empty

        private val instantDeserializer = longDeserializer.map(Instant.ofEpochMilli)
        private val durationDeserializer = longDeserializer.map(Duration(_, MILLISECONDS))

        override def retryTopicsFor(topic: Topic): Set[Topic] =
          (backoffs.indices.foldLeft(Set.empty[String])((acc, attempt) => acc + s"$topic-$group-retry-$attempt")) + topic

        override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]] = {
          (for {
            submitted <- headers.get(RetryHeader.Submitted, instantDeserializer)
            backoff <- headers.get(RetryHeader.Backoff, durationDeserializer)
            originalTopic <- headers.get[String](RetryHeader.OriginalTopic, StringSerde)
          } yield for {
            TopicAttempt(originalTopic, attempt) <- topicAttempt(subscription, topic, originalTopic)
            s <- submitted
            b <- backoff
          } yield RetryAttempt(originalTopic, attempt, s, b))
            .catchAll(_ => ZIO.none)
        }

        private def topicAttempt(subscription: ConsumerSubscription, topic: Topic, originalTopicHeader: Option[String]) =
          subscription match {
            case _: Topics => extractTopicAttempt(group, topic)
            case _: TopicPattern => extractTopicAttemptFromPatternRetryTopic(group, topic, originalTopicHeader)
          }

        override def retryDecision[E](retryAttempt: Option[RetryAttempt],
                                      record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
                                      error: E,
                                      subscription: ConsumerSubscription): URIO[Clock, RetryDecision] = {
          currentTime.map(now => {
            val nextRetryAttempt = retryAttempt.fold(0)(_.attempt + 1)
            val originalTopic = retryAttempt.fold(record.topic)(_.originalTopic)
            val retryTopic = subscription match {
              case _: TopicPattern => patternRetryTopic(group, nextRetryAttempt)
              case _: Topics => fixedRetryTopic(originalTopic, group, nextRetryAttempt)
            }
            backoffs.lift(nextRetryAttempt).map { backoff => {
              ProducerRecord(
                topic = retryTopic,
                value = record.value,
                key = record.key,
                partition = None,
                headers = record.headers +
                  (RetryHeader.Submitted -> toChunk(now.toEpochMilli)) +
                  (RetryHeader.Backoff -> toChunk(backoff.toMillis)) +
                  (RetryHeader.OriginalTopic -> toChunk(originalTopic))
              )
            }
            }.fold[RetryDecision](NoMoreRetries)(RetryWith)
          })
        }

        private def toChunk(long: Long): Chunk[Byte] =
          Chunk.fromArray(long.toString.getBytes)

        private def toChunk(str: String): Chunk[Byte] =
          Chunk.fromArray(str.getBytes)
      }

      private def extractTopicAttempt[E](group: Group, inputTopic: Topic) =
        inputTopic.split(s"-$group-retry-").toSeq match {
          case Seq(topic, attempt) if Try(attempt.toInt).isSuccess => Some(TopicAttempt(topic, attempt.toInt))
          case _ => None
        }

      private def extractTopicAttemptFromPatternRetryTopic[E](group: Group, inputTopic: Topic, originalTopicHeader: Option[String]) = {
        originalTopicHeader.flatMap(originalTopic => {
          inputTopic.split(s"__gh_pattern-retry-$group-attempt-").toSeq match {
            case Seq(_, attempt) if Try(attempt.toInt).isSuccess => Some(TopicAttempt(originalTopic, attempt.toInt))
            case _ => None
          }
        })
      }


  def retryPattern(group: Group) =
    s"__gh_pattern-retry-$group-attempt-\\d\\d*"

  def patternRetryTopic(group: Group, step: Int) =
    s"__gh_pattern-retry-$group-attempt-$step"

  def fixedRetryTopic(originalTopic: Topic, group: Group, nextRetryAttempt: Int) =
    s"$originalTopic-$group-retry-$nextRetryAttempt"
}