package com.wixpress.dst.greyhound.core.consumer

import java.time.Instant

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RecordHandlerTest._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.FakeRetryPolicy._
import com.wixpress.dst.greyhound.core.testkit._
import zio._
import zio.clock.Clock
import zio.duration._
import zio.random.{nextBytes, nextInt}
import zio.test.environment.{TestClock, TestEnvironment, TestRandom}

class RecordHandlerTest extends BaseTest[TestRandom with TestClock with TestMetrics] {

  override def env: UManaged[TestRandom with TestClock with TestMetrics] =
    for {
      env <- TestEnvironment.Value
      testMetrics <- TestMetrics.make
    } yield new TestRandom with TestClock with TestMetrics {
      override val random: TestRandom.Service[Any] = env.random
      override val clock: TestClock.Service[Any] = env.clock
      override val scheduler: TestClock.Service[Any] = env.scheduler
      override val metrics: TestMetrics.Service = testMetrics.metrics
    }

  "withRetries" should {
    "produce a message to the retry topic after failure" in {
      for {
        producer <- FakeProducer.make
        retryHandler = RetryRecordHandler.withRetries(failingHandler, retryPolicy, producer)
        key <- bytes
        value <- bytes
        _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
        record <- producer.records.take
        now <- currentTime
        retryAttempt <- IntSerde.serialize(retryTopic, 0)
        submittedAt <- InstantSerde.serialize(retryTopic, now)
        backoff <- DurationSerde.serialize(retryTopic, 1.second)
      } yield {
        record === ProducerRecord(retryTopic, value, Some(key),
          partition = None,
          headers = Headers(
            "retry-attempt" -> retryAttempt,
            "retry-submitted-at" -> submittedAt,
            "retry-backoff" -> backoff))
      }
    }

    "delay execution of user handler by configured backoff" in {
      for {
        producer <- FakeProducer.make
        executionTime <- Promise.make[Nothing, Instant]
        handler = RecordHandler[Clock, HandlerError, Chunk[Byte], Chunk[Byte]] { _ =>
          currentTime.flatMap(executionTime.succeed)
        }
        retryHandler = RetryRecordHandler.withRetries(handler, retryPolicy, producer)
        value <- bytes
        begin <- currentTime
        retryAttempt <- IntSerde.serialize(retryTopic, 0)
        submittedAt <- InstantSerde.serialize(retryTopic, begin)
        backoff <- DurationSerde.serialize(retryTopic, 1.second)
        headers = Headers(
          "retry-attempt" -> retryAttempt,
          "retry-submitted-at" -> submittedAt,
          "retry-backoff" -> backoff)
        _ <- retryHandler.handle(ConsumerRecord(retryTopic, partition, offset, headers, None, value, 0l, 0l, 0L)).fork
        _ <- TestClock.adjust(1.second)
        end <- executionTime.await
      } yield end must equalTo(begin.plusSeconds(1))
    }
  }
}

object RecordHandlerTest {
  val topic: Topic = "some-topic"
  val retryTopic = s"$topic-retry"
  val group = "some-group"
  val partition = 0
  val offset = 0L
  val bytes = nextInt(9).flatMap(size => nextBytes(size + 1))

  val failingHandler = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](_ => ZIO.fail(RetriableError))

  val retryPolicy = FakeRetryPolicy(topic)
}