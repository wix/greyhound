package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.retry.ExponentialBackoffCalculator.exponentialBackoffs
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import zio.Chunk
import zio.duration.{Duration => ZDuration}

import scala.concurrent.duration.Duration

case class RetryConfig(configs: PartialFunction[Topic, RetryConfigForTopic]) {
  def blockingBackoffs(topic: Topic) =
    configs(topic).blockingBackoffs

  def retryType(topic: Topic) =
    if (configs.isDefinedAt(topic)) configs(topic).retryType else NoRetries

  def withCustomRetriesFor(overrideConfigs: PartialFunction[Topic, RetryConfigForTopic]): RetryConfig =
    copy(configs = {
      case t =>
        if (overrideConfigs.isDefinedAt(t)) overrideConfigs(t) else configs(t)
    })

  def nonBlockingBackoffs(topic: Topic): NonBlockingBackoffPolicy =
    configs(topic).nonBlockingBackoffs
}

case class NonBlockingBackoffPolicy(intervals: Seq[ZDuration],
                                    recordMutate: ProducerRecord[Chunk[Byte], Chunk[Byte]] => ProducerRecord[Chunk[Byte], Chunk[Byte]] = identity) {
  def nonEmpty = intervals.nonEmpty

  def length = intervals.length
}

object NonBlockingBackoffPolicy {
  val empty = NonBlockingBackoffPolicy(Nil)
}

case class RetryConfigForTopic(blockingBackoffs: () => Seq[ZDuration], nonBlockingBackoffs: NonBlockingBackoffPolicy) {

  def retryType: RetryType =
    if (blockingBackoffs.apply().nonEmpty) {
      if (nonBlockingBackoffs.nonEmpty)
        BlockingFollowedByNonBlocking
      else
        Blocking
    } else {
      NonBlocking
    }
}

object RetryConfigForTopic {
  val empty = RetryConfigForTopic(() => Nil, NonBlockingBackoffPolicy.empty)
}

object ZRetryConfig {
  def nonBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    forAllTopics(RetryConfigForTopic(nonBlockingBackoffs = NonBlockingBackoffPolicy(firstRetry :: otherRetries.toList), blockingBackoffs = () => List.empty))

  def finiteBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    forAllTopics(RetryConfigForTopic(blockingBackoffs = () => firstRetry :: otherRetries.toList, nonBlockingBackoffs = NonBlockingBackoffPolicy.empty))

  def infiniteBlockingRetry(interval: ZDuration): RetryConfig =
    forAllTopics(RetryConfigForTopic(blockingBackoffs = () => Stream.continually(interval), nonBlockingBackoffs = NonBlockingBackoffPolicy.empty))

  def exponentialBackoffBlockingRetry(initialInterval: ZDuration,
                                      maximalInterval: ZDuration,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    forAllTopics(RetryConfigForTopic(blockingBackoffs = () => exponentialBackoffs(initialInterval, maximalInterval,
      backOffMultiplier, infiniteRetryMaxInterval),
      nonBlockingBackoffs = NonBlockingBackoffPolicy.empty))

  def exponentialBackoffBlockingRetry(initialInterval: ZDuration,
                                      maxMultiplications: Int,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    forAllTopics(RetryConfigForTopic(blockingBackoffs = () => exponentialBackoffs(initialInterval, maxMultiplications,
      backOffMultiplier, infiniteRetryMaxInterval),

      nonBlockingBackoffs = NonBlockingBackoffPolicy.empty))

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: NonEmptyList[ZDuration], nonBlockingBackoffs: NonBlockingBackoffPolicy): RetryConfig =
    forAllTopics(RetryConfigForTopic(blockingBackoffs = () => blockingBackoffs, nonBlockingBackoffs = nonBlockingBackoffs))

  def perTopicRetries(configs: PartialFunction[Topic, RetryConfigForTopic]) =
    RetryConfig(configs)

  private def forAllTopics(config: RetryConfigForTopic): RetryConfig =
    RetryConfig { case _ => config }
}

object RetryConfig {
  def nonBlockingRetry(firstRetry: Duration, otherRetries: Duration*): RetryConfig =
    ZRetryConfig.nonBlockingRetry(ZDuration.fromScala(firstRetry), otherRetries.toList.map(ZDuration.fromScala): _*)

  def finiteBlockingRetry(firstRetry: Duration, otherRetries: Duration*): RetryConfig =
    ZRetryConfig.finiteBlockingRetry(ZDuration.fromScala(firstRetry), otherRetries.toList.map(ZDuration.fromScala): _*)

  def infiniteBlockingRetry(interval: Duration): RetryConfig =
    ZRetryConfig.infiniteBlockingRetry(ZDuration.fromScala(interval))

  def exponentialBackoffBlockingRetry(initialInterval: ZDuration,
                                      maximalInterval: ZDuration,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    ZRetryConfig.exponentialBackoffBlockingRetry(initialInterval, maximalInterval, backOffMultiplier, infiniteRetryMaxInterval)

  def exponentialBackoffBlockingRetry(initialInterval: ZDuration,
                                      maxMultiplications: Int,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    ZRetryConfig.exponentialBackoffBlockingRetry(initialInterval, maxMultiplications, backOffMultiplier, infiniteRetryMaxInterval)

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: NonEmptyList[Duration], nonBlockingBackoffs: List[Duration]): RetryConfig =
    ZRetryConfig.blockingFollowedByNonBlockingRetry(blockingBackoffs = blockingBackoffs.map(ZDuration.fromScala), nonBlockingBackoffs = NonBlockingBackoffPolicy(nonBlockingBackoffs.map(ZDuration.fromScala)))


  def perTopicRetryConfig(configs: PartialFunction[Topic, RetryConfigForTopic]) =
    ZRetryConfig.perTopicRetries(configs)
}

trait RetryType

case object Blocking extends RetryType

case object NonBlocking extends RetryType

case object BlockingFollowedByNonBlocking extends RetryType

case object NoRetries extends RetryType

case class NonRetriableException(cause: Exception) extends Exception(cause)

case object BlockingHandlerFailed extends RuntimeException
