package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.retry.ExponentialBackoffCalculator.exponentialBackoffs
import zio.duration.{Duration => ZDuration}

import scala.concurrent.duration.Duration

case class RetryConfig(blockingBackoffs: () => Seq[ZDuration], nonBlockingBackoffs: Seq[ZDuration]) {
  def retryType: RetryType = {
    if (blockingBackoffs.apply().nonEmpty) {
      if (nonBlockingBackoffs.nonEmpty)
        BlockingFollowedByNonBlocking
      else
        Blocking
    } else {
      NonBlocking
    }
  }
}

object ZRetryConfig {
  def nonBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    RetryConfig(nonBlockingBackoffs = firstRetry :: otherRetries.toList, blockingBackoffs = () => List.empty)

  def finiteBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    RetryConfig(blockingBackoffs = () => firstRetry :: otherRetries.toList, nonBlockingBackoffs = List.empty)

  def infiniteBlockingRetry(interval: ZDuration): RetryConfig =
    RetryConfig(blockingBackoffs = () => Stream.continually(interval), nonBlockingBackoffs = List.empty)

  def exponentialBackoffBlockingRetry(initialInterval: ZDuration,
                                      maximalInterval: ZDuration,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    RetryConfig(blockingBackoffs = () => exponentialBackoffs(initialInterval, maximalInterval,
      backOffMultiplier, infiniteRetryMaxInterval),
      nonBlockingBackoffs = List.empty)

  def exponentialBackoffBlockingRetry(initialInterval: ZDuration,
                                      maxMultiplications: Int,
                                      backOffMultiplier: Float,
                                      infiniteRetryMaxInterval: Boolean): RetryConfig =
    RetryConfig(blockingBackoffs = () => exponentialBackoffs(initialInterval, maxMultiplications,
      backOffMultiplier, infiniteRetryMaxInterval),
      nonBlockingBackoffs = List.empty)

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: NonEmptyList[ZDuration], nonBlockingBackoffs: List[ZDuration]): RetryConfig =
    RetryConfig(blockingBackoffs = () => blockingBackoffs, nonBlockingBackoffs = nonBlockingBackoffs)
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
    ZRetryConfig.blockingFollowedByNonBlockingRetry(blockingBackoffs = blockingBackoffs.map(ZDuration.fromScala), nonBlockingBackoffs = nonBlockingBackoffs.map(ZDuration.fromScala))
}

trait RetryType

case object Blocking extends RetryType

case object NonBlocking extends RetryType

case object BlockingFollowedByNonBlocking extends RetryType

case class NonRetriableException(cause: Exception) extends Exception(cause)

case object BlockingHandlerFailed extends RuntimeException
