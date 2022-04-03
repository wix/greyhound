package com.wixpress.dst.greyhound.core.consumer.retry

import java.nio.charset.StandardCharsets
import java.time.Instant

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.topics
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.RetryWith
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import com.wixpress.dst.greyhound.core.testkit.Maker.abytesRecord
import zio.duration._
import zio.test.environment.TestEnvironment
import zio._

class NonBlockingRetryHelperTest extends BaseTest[TestEnvironment] {

  "NonBlockingRetryHelper" should {
    "retryTopicsFor should not contain original topic" in {
      val topic = "some-topic"
      NonBlockingRetryHelper("group", None).retryTopicsFor(topic) must not(contain(topic))
    }

    "retryDecision should produce add a 'current RetryAttempt' header to ProducerRecord" in {
      for {
        record <- abytesRecord
        retryAttempt = 0
        decision <- NonBlockingRetryHelper("group", Some(ZRetryConfig.nonBlockingRetry(1.millisecond, 2.millis))).retryDecision(
          Some(RetryAttempt("some-topic", retryAttempt, Instant.ofEpochMilli(0), 1.second)),
          record,
          (),
          topics(record.topic)
        )
        producerRecord = decision match {
          case r: RetryWith => Some(r.record)
          case _            => None
        }
        maybeHeader <- ZIO.fromOption(producerRecord.flatMap(_.headers.headers.get("GH_RetryAttempt")))
        attempt     <- makeString(maybeHeader)
      } yield attempt === (retryAttempt + 1).toString
    }

    "retryDecision should produce add a 'RetryAttempt' 0 header to ProducerRecord for empty RetryAttempt param" in {
      for {
        record <- abytesRecord
        decision <- NonBlockingRetryHelper("group", Some(ZRetryConfig.nonBlockingRetry(1.millisecond, 2.millis)))
          .retryDecision(None, record, (), topics(record.topic))
        producerRecord = decision match {
          case r: RetryWith => Some(r.record)
          case _            => None
        }
        maybeHeader <- ZIO.fromOption(producerRecord.flatMap(_.headers.headers.get("GH_RetryAttempt")))
        attempt     <- makeString(maybeHeader)
      } yield attempt === "0"
    }
  }

  override def env: UManaged[TestEnvironment] = test.environment.testEnvironment.build

  def makeString(value: Chunk[Byte]) =
    IO(new String(value.toArray, StandardCharsets.US_ASCII))
}
