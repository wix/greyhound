package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.consumer.retry.ExponentialBackoffCalculator.exponentialBackoffs
import com.wixpress.dst.greyhound.core.producer.{Encryptor, NoOpEncryptor, ProducerRecord}
import zio.{Chunk, UIO, ZIO}
import zio.{Duration => ZDuration}

import scala.concurrent.duration._

case class RetryConfig(
  perTopic: PartialFunction[Topic, RetryConfigForTopic],
  forPatternSubscription: Option[RetryConfigForTopic],
  produceRetryBackoff: Duration = 5.seconds,
  produceEncryptor: ConsumerRecord[_, _] => UIO[Encryptor] = _ => ZIO.succeed(NoOpEncryptor)(zio.Trace.empty)
) {
  def blockingBackoffs(topic: Topic): Seq[ZDuration] =
    get(topic) {
      case RetryConfigForTopic(FiniteBlockingBackoffPolicy(intervals), _, _) if intervals.nonEmpty => intervals
      case RetryConfigForTopic(_, InfiniteFixedBackoff(fixed), _)             => Stream.continually(fixed)
      case RetryConfigForTopic(_, expMax: InfiniteExponentialBackoffsMaxInterval, _)            =>
        exponentialBackoffs(expMax.initialInterval, expMax.maximalInterval, expMax.backOffMultiplier, infiniteRetryMaxInteval = true)
      case RetryConfigForTopic(_, expMult: InfiniteExponentialBackoffsMaxMultiplication, _)           =>
        exponentialBackoffs(
          expMult.initialInterval,
          expMult.maxMultiplications,
          expMult.backOffMultiplier,
          infiniteRetryMaxInterval = true
        )
      case _                                                                                       => Nil
    }(ifEmpty = Nil)

  def finiteBlockingBackoffs(topic: Topic) =
    get(topic)(_.finiteBlockingBackoffs)(ifEmpty = FiniteBlockingBackoffPolicy.empty)

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
    forPatternSubscription
      .map(f)
      .getOrElse(
        if (perTopic.isDefinedAt(forTopic))
          f(perTopic(forTopic))
        else
          ifEmpty
      )
}

trait InfiniteBlockingBackoffPolicy {
  def nonEmpty: Boolean
}

case object EmptyInfiniteBlockingBackoffPolicy extends InfiniteBlockingBackoffPolicy {
  override def nonEmpty: Boolean = false
}

sealed case class InfiniteFixedBackoff(interval: ZDuration) extends InfiniteBlockingBackoffPolicy {
  override def nonEmpty: Boolean = true
}

sealed case class InfiniteExponentialBackoffsMaxInterval(
  initialInterval: ZDuration,
  maximalInterval: ZDuration,
  backOffMultiplier: Float
) extends InfiniteBlockingBackoffPolicy {
  override def nonEmpty: Boolean = true
}

sealed case class InfiniteExponentialBackoffsMaxMultiplication(
  initialInterval: ZDuration,
  maxMultiplications: Int,
  backOffMultiplier: Float
) extends InfiniteBlockingBackoffPolicy {
  override def nonEmpty: Boolean = true
}

case class FiniteBlockingBackoffPolicy(intervals: List[ZDuration]) {
  def nonEmpty = intervals.nonEmpty
}

object FiniteBlockingBackoffPolicy {
  val empty = FiniteBlockingBackoffPolicy(Nil)
}

case class NonBlockingBackoffPolicy(
  intervals: List[ZDuration],
  recordMutate: ProducerRecord[Chunk[Byte], Chunk[Byte]] => ProducerRecord[Chunk[Byte], Chunk[Byte]] = identity
) {
  def nonEmpty = intervals.nonEmpty

  def length = intervals.length
}

object NonBlockingBackoffPolicy {
  val empty = NonBlockingBackoffPolicy(Nil)
}

case class RetryConfigForTopic(
  finiteBlockingBackoffs: FiniteBlockingBackoffPolicy,
  infiniteBlockingBackoffs: InfiniteBlockingBackoffPolicy,
  nonBlockingBackoffs: NonBlockingBackoffPolicy
) {
  def nonEmpty: Boolean = finiteBlockingBackoffs.nonEmpty || infiniteBlockingBackoffs.nonEmpty || nonBlockingBackoffs.nonEmpty

  def retryType: RetryType =
    if (finiteBlockingBackoffs.nonEmpty) {
      if (nonBlockingBackoffs.nonEmpty)
        BlockingFollowedByNonBlocking
      else
        Blocking
    } else if (infiniteBlockingBackoffs.nonEmpty)
      Blocking
    else {
      NonBlocking
    }
}

object RetryConfigForTopic {
  val empty = RetryConfigForTopic(FiniteBlockingBackoffPolicy.empty, EmptyInfiniteBlockingBackoffPolicy, NonBlockingBackoffPolicy.empty)

  def nonBlockingRetryConfigForTopic(intervals: List[ZDuration]) =
    RetryConfigForTopic(FiniteBlockingBackoffPolicy.empty, EmptyInfiniteBlockingBackoffPolicy, NonBlockingBackoffPolicy(intervals))

  def finiteBlockingRetryConfigForTopic(intervals: List[ZDuration]) =
    RetryConfigForTopic(FiniteBlockingBackoffPolicy(intervals), EmptyInfiniteBlockingBackoffPolicy, NonBlockingBackoffPolicy.empty)

  def infiniteBlockingRetryConfigForTopic(interval: ZDuration) =
    RetryConfigForTopic(FiniteBlockingBackoffPolicy.empty, InfiniteFixedBackoff(interval), NonBlockingBackoffPolicy.empty)

  def infiniteBlockingRetryConfigForTopic(
    initialInterval: ZDuration,
    maxInterval: ZDuration,
    backOffMultiplier: Float
  ) =
    RetryConfigForTopic(
      FiniteBlockingBackoffPolicy.empty,
      InfiniteExponentialBackoffsMaxInterval(initialInterval, maxInterval, backOffMultiplier),
      NonBlockingBackoffPolicy.empty
    )
}

object ZRetryConfig {
  def nonBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    forAllTopics(
      RetryConfigForTopic(
        nonBlockingBackoffs = NonBlockingBackoffPolicy(firstRetry :: otherRetries.toList),
        finiteBlockingBackoffs = FiniteBlockingBackoffPolicy.empty,
        infiniteBlockingBackoffs = EmptyInfiniteBlockingBackoffPolicy
      )
    )

  def finiteBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    forAllTopics(
      RetryConfigForTopic(
        finiteBlockingBackoffs = FiniteBlockingBackoffPolicy(firstRetry :: otherRetries.toList),
        infiniteBlockingBackoffs = EmptyInfiniteBlockingBackoffPolicy,
        nonBlockingBackoffs = NonBlockingBackoffPolicy.empty
      )
    )

  def infiniteBlockingRetry(interval: ZDuration): RetryConfig =
    forAllTopics(
      RetryConfigForTopic(
        infiniteBlockingBackoffs = InfiniteFixedBackoff(interval),
        finiteBlockingBackoffs = FiniteBlockingBackoffPolicy.empty,
        nonBlockingBackoffs = NonBlockingBackoffPolicy.empty
      )
    )

  def exponentialBackoffBlockingRetry(
    initialInterval: ZDuration,
    maximalInterval: ZDuration,
    backOffMultiplier: Float,
    infiniteRetryMaxInterval: Boolean
  ): RetryConfig = {
    val (finite, infinite) = if (infiniteRetryMaxInterval) {
      (
        FiniteBlockingBackoffPolicy.empty,
        InfiniteExponentialBackoffsMaxInterval(initialInterval, maximalInterval, backOffMultiplier)
      )
    } else {
      (
        FiniteBlockingBackoffPolicy(
          exponentialBackoffs(initialInterval, maximalInterval, backOffMultiplier, infiniteRetryMaxInterval).toList
        ),
        EmptyInfiniteBlockingBackoffPolicy
      )
    }
    forAllTopics(
      RetryConfigForTopic(
        finiteBlockingBackoffs = finite,
        infiniteBlockingBackoffs = infinite,
        nonBlockingBackoffs = NonBlockingBackoffPolicy.empty
      )
    )
  }

  def exponentialBackoffBlockingRetry(
    initialInterval: ZDuration,
    maxMultiplications: Int,
    backOffMultiplier: Float,
    infiniteRetryMaxInterval: Boolean
  ): RetryConfig = {
    val (finite, infinite) = if (infiniteRetryMaxInterval) {
      (
        FiniteBlockingBackoffPolicy.empty,
        InfiniteExponentialBackoffsMaxMultiplication(initialInterval, maxMultiplications, backOffMultiplier)
      )
    } else {
      (
        FiniteBlockingBackoffPolicy(
          exponentialBackoffs(initialInterval, maxMultiplications, backOffMultiplier, infiniteRetryMaxInterval).toList
        ),
        EmptyInfiniteBlockingBackoffPolicy
      )
    }
    forAllTopics(
      RetryConfigForTopic(
        finiteBlockingBackoffs = finite,
        infiniteBlockingBackoffs = infinite,
        nonBlockingBackoffs = NonBlockingBackoffPolicy.empty
      )
    )
  }

  def blockingFollowedByNonBlockingRetry(
    blockingBackoffs: FiniteBlockingBackoffPolicy,
    nonBlockingBackoffs: NonBlockingBackoffPolicy
  ): RetryConfig =
    forAllTopics(
      RetryConfigForTopic(
        finiteBlockingBackoffs = blockingBackoffs,
        nonBlockingBackoffs = nonBlockingBackoffs,
        infiniteBlockingBackoffs = EmptyInfiniteBlockingBackoffPolicy
      )
    )

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

  def exponentialBackoffBlockingRetry(
    initialInterval: Duration,
    maximalInterval: Duration,
    backOffMultiplier: Float,
    infiniteRetryMaxInterval: Boolean
  ): RetryConfig =
    ZRetryConfig.exponentialBackoffBlockingRetry(
      ZDuration.fromScala(initialInterval),
      ZDuration.fromScala(maximalInterval),
      backOffMultiplier,
      infiniteRetryMaxInterval
    )

  def exponentialBackoffBlockingRetry(
    initialInterval: Duration,
    maxMultiplications: Int,
    backOffMultiplier: Float,
    infiniteRetryMaxInterval: Boolean
  ): RetryConfig =
    ZRetryConfig.exponentialBackoffBlockingRetry(
      ZDuration.fromScala(initialInterval),
      maxMultiplications,
      backOffMultiplier,
      infiniteRetryMaxInterval
    )

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: NonEmptyList[Duration], nonBlockingBackoffs: List[Duration]): RetryConfig =
    ZRetryConfig.blockingFollowedByNonBlockingRetry(
      blockingBackoffs = FiniteBlockingBackoffPolicy(blockingBackoffs.map(ZDuration.fromScala)),
      nonBlockingBackoffs = NonBlockingBackoffPolicy(nonBlockingBackoffs.map(ZDuration.fromScala))
    )
}

trait RetryType

case object Blocking extends RetryType

case object NonBlocking extends RetryType

case object BlockingFollowedByNonBlocking extends RetryType

case object NoRetries extends RetryType

case class NonRetriableException(cause: Exception) extends Exception(cause)

case object BlockingHandlerFailed extends RuntimeException
