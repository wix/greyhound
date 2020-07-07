package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core._
import zio.duration.{Duration => ZDuration}

import scala.concurrent.duration.Duration


case class RetryConfig(retryType: RetryType, blockingBackoffs: () => Seq[ZDuration], nonBlockingBackoffs: Seq[ZDuration])

object ZRetryConfig {
  def nonBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    RetryConfig(retryType = NonBlocking, nonBlockingBackoffs = firstRetry :: otherRetries.toList, blockingBackoffs = () => List.empty)

  def finiteBlockingRetry(firstRetry: ZDuration, otherRetries: ZDuration*): RetryConfig =
    RetryConfig(retryType = Blocking, blockingBackoffs = () => firstRetry :: otherRetries.toList, nonBlockingBackoffs = List.empty)

  def infiniteBlockingRetry(interval: ZDuration): RetryConfig =
    RetryConfig(retryType = Blocking, blockingBackoffs = () => Stream.continually(interval), nonBlockingBackoffs = List.empty)

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: NonEmptyList[ZDuration], nonBlockingBackoffs: List[ZDuration]): RetryConfig =
    RetryConfig(retryType = BlockingFollowedByNonBlocking, blockingBackoffs = () => blockingBackoffs, nonBlockingBackoffs = nonBlockingBackoffs)
}

object RetryConfig {
  def nonBlockingRetry(firstRetry: Duration, otherRetries: Duration*): RetryConfig =
    ZRetryConfig.nonBlockingRetry(ZDuration.fromScala(firstRetry), otherRetries.toList.map(ZDuration.fromScala):_*)

  def finiteBlockingRetry(firstRetry: Duration, otherRetries: Duration*): RetryConfig =
    ZRetryConfig.finiteBlockingRetry(ZDuration.fromScala(firstRetry), otherRetries.toList.map(ZDuration.fromScala):_*)

  def infiniteBlockingRetry(interval: Duration): RetryConfig =
    ZRetryConfig.infiniteBlockingRetry(ZDuration.fromScala(interval))

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: NonEmptyList[Duration], nonBlockingBackoffs: List[Duration]): RetryConfig =
    ZRetryConfig.blockingFollowedByNonBlockingRetry(blockingBackoffs = blockingBackoffs.map(ZDuration.fromScala), nonBlockingBackoffs = nonBlockingBackoffs.map(ZDuration.fromScala))
}

trait RetryType

case object Blocking extends RetryType

case object NonBlocking extends RetryType

case object BlockingFollowedByNonBlocking extends RetryType

case class NonRetryableException(cause: Exception) extends Exception(cause)
case object BlockingHandlerFailed extends RuntimeException