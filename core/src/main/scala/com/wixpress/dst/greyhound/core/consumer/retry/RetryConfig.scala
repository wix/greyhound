package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.retry.ExponentialBackoffCalculator.exponentialBackoffs
import com.wixpress.dst.greyhound.core.producer.{Encryptor, NoOpEncryptor, ProducerRecord}
import zio.Chunk
import zio.duration.{Duration => ZDuration}

import scala.concurrent.duration._

case class RetryConfig(perTopic: PartialFunction[Topic, RetryConfigForTopic],
                       forPatternSubscription: Option[RetryConfigForTopic],
                       produceRetryBackoff: Duration = 5.seconds,
                       produceEncryptor: Encryptor = NoOpEncryptor
                      ) {
  def blockingBackoffs(topic: Topic) =
    get(topic)(_.blockingBackoffs)(ifEmpty = () => Nil)

  def retryType(originalTopic: Topic): RetryType =
    get(originalTopic)(_.retryType)(ifEmpty = NoRetries)

  def nonBlockingBackoffs(topic: Topic): NonBlockingBackoffPolicy =
    get(topic)(_.nonBlockingBackoffs)(ifEmpty = NonBlockingBackoffPolicy.empty)

  def withCustomRetriesFor(overrideConfigs: PartialFunction[Topic, RetryConfigForTopic]): RetryConfig = {
    val newConfigs: PartialFunction[Topic, RetryConfigForTopic] = {
      case t => if (overrideConfigs.isDefinedAt(t)) overrideConfigs(t) else perTopic(t)
    }

    copy(perTopic = newConfigs)
  }

  def withProduceRetryBackoff(duration: Duration) = copy(produceRetryBackoff = duration)

  private def get[T](forTopic: Topic)(f: RetryConfigForTopic => T)(ifEmpty: => T): T =
    forPatternSubscription.map(f).getOrElse(
      if (perTopic.isDefinedAt(forTopic))
        f(perTopic(forTopic))
      else
        ifEmpty)

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
  def nonEmpty: Boolean = blockingBackoffs().nonEmpty || nonBlockingBackoffs.nonEmpty

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
    RetryConfig(configs, None)

  def retryForPattern(config: RetryConfigForTopic) =
    RetryConfig(Map.empty, Some(config))

  private def forAllTopics(config: RetryConfigForTopic): RetryConfig =
    RetryConfig({ case _ => config }, None)
}

object RetryConfig {
  val empty = RetryConfig(Map.empty, None)

  def nonBlockingRetry(firstRetry: Duration, otherRetries: Duration*): RetryConfig =
    ZRetryConfig.nonBlockingRetry(ZDuration.fromScala(firstRetry), otherRetries.toList.map(ZDuration.fromScala): _*)

  def finiteBlockingRetry(firstRetry: Duration, otherRetries: Duration*): RetryConfig =
    ZRetryConfig.finiteBlockingRetry(ZDuration.fromScala(firstRetry), otherRetries.toList.map(ZDuration.fromScala): _*)

  def infiniteBlockingRetry(interval: Duration): RetryConfig =
    ZRetryConfig.infiniteBlockingRetry(ZDuration.fromScala(interval))

  def exponentialBackoffBlockingRetry(initialInterval: Duration,
                                      maximalInterval: Duration,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    ZRetryConfig.exponentialBackoffBlockingRetry(ZDuration.fromScala(initialInterval), ZDuration.fromScala(maximalInterval), backOffMultiplier, infiniteRetryMaxInterval)

  def exponentialBackoffBlockingRetry(initialInterval: Duration,
                                      maxMultiplications: Int,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    ZRetryConfig.exponentialBackoffBlockingRetry(ZDuration.fromScala(initialInterval), maxMultiplications, backOffMultiplier, infiniteRetryMaxInterval)

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: NonEmptyList[Duration], nonBlockingBackoffs: List[Duration]): RetryConfig =
    ZRetryConfig.blockingFollowedByNonBlockingRetry(blockingBackoffs = blockingBackoffs.map(ZDuration.fromScala), nonBlockingBackoffs = NonBlockingBackoffPolicy(nonBlockingBackoffs.map(ZDuration.fromScala)))
}

trait RetryType

case object Blocking extends RetryType

case object NonBlocking extends RetryType

case object BlockingFollowedByNonBlocking extends RetryType

case object NoRetries extends RetryType

case class NonRetriableException(cause: Exception) extends Exception(cause)

case object BlockingHandlerFailed extends RuntimeException
