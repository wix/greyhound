package com.wixpress.dst.greyhound.java

import java.time.Duration
import java.util
import java.util.List

import com.wixpress.dst.greyhound.core.consumer.retry.ZRetryConfig
import java.util.Collections
import scala.collection.JavaConverters._

object RetryConfigBuilder {
  def nonBlockingRetry(nonBlockingBackoffs: List[Duration]): RetryConfig = {
    new RetryConfig(Collections.emptyList[Duration], nonBlockingBackoffs)
  }

  def finiteBlockingRetry(blockingBackoffs: List[Duration]): RetryConfig = {
    new RetryConfig(blockingBackoffs, Collections.emptyList[Duration])
  }

  def blockingFollowedByNonBlockingRetry(blockingBackoffs: List[Duration], nonBlockingBackoffs: List[Duration]): RetryConfig = {
    new RetryConfig(blockingBackoffs, nonBlockingBackoffs)
  }

  def exponentialBackoffBlockingRetry(initialInterval: Duration, maximalInterval: Duration, backOffMultiplier: Float, infiniteRetryMaxInterval: Boolean): RetryConfig = {
    fromCoreRetryConfig(ZRetryConfig.exponentialBackoffBlockingRetry(initialInterval, maximalInterval, backOffMultiplier, infiniteRetryMaxInterval))
  }

  def exponentialBackoffBlockingRetry(initialInterval: Duration, maxMultiplications: Int, backOffMultiplier: Float, infiniteRetryMaxInterval: Boolean): RetryConfig = {
    fromCoreRetryConfig(ZRetryConfig.exponentialBackoffBlockingRetry(initialInterval, maxMultiplications, backOffMultiplier, infiniteRetryMaxInterval))
  }

  private def fromCoreRetryConfig(coreRetryConfig: com.wixpress.dst.greyhound.core.consumer.retry.RetryConfig): RetryConfig = {
    val blocking: util.List[Duration] = seqAsJavaList(coreRetryConfig.blockingBackoffs("").apply)
    val nonBlocking: util.List[Duration] = seqAsJavaList(coreRetryConfig.nonBlockingBackoffs("").intervals)
    new RetryConfig(blocking, nonBlocking)
  }
}
