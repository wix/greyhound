package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import zio.test.TestEnvironment

import java.time.{Duration, Instant}
import scala.util.Random
import scala.concurrent.duration._

class RetryAttemptTest extends BaseTest[TestEnvironment] {

  "RetryAttempt.extract" should {
    "deserialize attempt from headers for blocking retries" in {
      val attempt = randomRetryAttempt
      val headers = RetryAttempt.toHeaders(attempt)
      val subscription = ConsumerSubscription.Topics(Set(attempt.originalTopic))
      for (result <- RetryAttempt.extract(headers, attempt.originalTopic, randomStr, subscription, None))
        yield result must beSome(attempt)
    }
    "deserialize attempt from headers and topic for non-blocking retries" in {
      val attempt = randomRetryAttempt
      // topic and attempt must be extracted from retryTopic
      val headers = RetryAttempt.toHeaders(attempt.copy(originalTopic = "", attempt = -1))
      val subscription = ConsumerSubscription.Topics(Set(attempt.originalTopic))
      val group = randomStr
      val retryTopic = NonBlockingRetryHelper.fixedRetryTopic(attempt.originalTopic, group, attempt.attempt)
      for (result <- RetryAttempt.extract(headers, retryTopic, group, subscription, None))
        yield result must beSome(attempt)
    }
    "deserialize attempt for non-blocking retry after blocking retries" in {
      val attempt      = randomRetryAttempt
      val headers      = RetryAttempt.toHeaders(attempt)
      val subscription = ConsumerSubscription.Topics(Set(attempt.originalTopic))
      val group        = randomStr
      val retries = RetryConfig.blockingFollowedByNonBlockingRetry(
        blockingBackoffs = 1.milli :: 1.second :: Nil,
        nonBlockingBackoffs = 5.minutes :: Nil,
      )
      val retryTopic   = NonBlockingRetryHelper.fixedRetryTopic(attempt.originalTopic, group, attempt.attempt)
      for (result <- RetryAttempt.extract(headers, retryTopic, group, subscription, Some(retries)))
        yield result must beSome(attempt.copy(attempt = attempt.attempt + 2)) // with 2 blocking retries before
    }
  }

  "RetryAttempt.maxOverallAttempts" should {
    "return 0 if no retries configured" in {
      RetryAttempt.maxOverallAttempts(randomStr, None) must beSome(0)
    }
    "return max attempts for blocking retries" in {
      val config = RetryConfig.finiteBlockingRetry(1.milli, 1.second)
      RetryAttempt.maxOverallAttempts(randomStr, Some(config)) must beSome(2)
    }
    "return max attempts for non-blocking retries" in {
      val config = RetryConfig.nonBlockingRetry(1.milli, 1.second, 5.minutes)
      RetryAttempt.maxOverallAttempts(randomStr, Some(config)) must beSome(3)
    }
    "return max attempts for blocking retries followed by non-blocking" in {
      val config = RetryConfig.blockingFollowedByNonBlockingRetry(1.milli :: 2.seconds :: Nil, 1.minute :: Nil)
      RetryAttempt.maxOverallAttempts(randomStr, Some(config)) must beSome(3)
    }
    "return None for infinite blocking retries" in {
      val config = RetryConfig.infiniteBlockingRetry(1.milli)
      RetryAttempt.maxOverallAttempts(randomStr, Some(config)) must beNone
    }
  }

  override def env = testEnvironment

  private def randomStr = Random.alphanumeric.take(10).mkString

  private def randomRetryAttempt = RetryAttempt(
    originalTopic = randomStr,
    attempt = Random.nextInt(1000),
    submittedAt = Instant.ofEpochMilli(math.abs(Random.nextLong())),
    backoff = Duration.ofMillis(Random.nextInt(100000))
  )
}
