package com.wixpress.dst.greyhound.core.consumer

import java.time.Instant

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RecordHandlerTest._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.FakeRetryPolicy._
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
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
    "add retry topics" in {
      FakeProducer.make.map { producer =>
        val handler = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](topic)(_ => ZIO.unit)
        val retryHandler = handler.withRetries(retryPolicy, producer)

        retryHandler.topics must equalTo(Set(topic, retryTopic))
      }
    }

    "produce a message to the retry topic after failure" in {
      for {
        producer <- FakeProducer.make
        retryHandler = failingHandler.withRetries(retryPolicy, producer)
        key <- bytes
        value <- bytes
        _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0l, 0l, 0L))
        record <- producer.records.take
        now <- currentTime
        retryAttempt <- IntSerde.serialize(retryTopic, 0)
        submittedAt <- InstantSerde.serialize(retryTopic, now)
        backoff <- DurationSerde.serialize(retryTopic, 1.second)
      } yield record must equalTo(
        ProducerRecord(
          topic = retryTopic,
          value = value,
          key = Some(key),
          partition = None,
          headers = Headers(
            "retry-attempt" -> retryAttempt,
            "retry-submitted-at" -> submittedAt,
            "retry-backoff" -> backoff)))
    }

    "fail when producer fails" in {
      for {
        producer <- FakeProducer.make
        retryHandler = failingHandler.withRetries(retryPolicy, producer.failing)
        value <- bytes
        result <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, None, value, 0l, 0l, 0L)).flip
      } yield result must beLeft
    }

    "delay execution of user handler by configured backoff" in {
      for {
        producer <- FakeProducer.make
        executionTime <- Promise.make[Nothing, Instant]
        handler = RecordHandler[Clock, HandlerError, Chunk[Byte], Chunk[Byte]](topic) { _ =>
          currentTime.flatMap(executionTime.succeed)
        }
        retryHandler = handler.withRetries(retryPolicy, producer)
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

    "fail with original error when retry p0Licy decides not to continue" in {
      for {
        producer <- FakeProducer.make
        failingHandler = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](topic)(_ => ZIO.fail(NonRetriableError))
        retryHandler = failingHandler.withRetries(retryPolicy, producer)
        value <- bytes
        result <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, None, value, 0l, 0l, 0L)).flip
      } yield result must beRight[HandlerError](NonRetriableError)
    }
  }

  "combine" should {
    "consume topics from both handlers" in {
      val handler =
        RecordHandler[Any, Nothing, Nothing, String]("topic1")(_ => ZIO.unit) combine
          RecordHandler[Any, Nothing, Nothing, String]("topic2")(_ => ZIO.unit)

      handler.topics must equalTo(Set("topic1", "topic2"))
    }

    "invoke the correct handler by topic" in {
      for {
        records1 <- Queue.unbounded[ConsumerRecord[Nothing, String]]
        handler1 = RecordHandler[Any, Nothing, Nothing, String]("topic1")(records1.offer)

        records2 <- Queue.unbounded[ConsumerRecord[Nothing, String]]
        handler2 = RecordHandler[Any, Nothing, Nothing, String]("topic2")(records2.offer)

        combined = handler1 combine handler2

        _ <- combined.handle(ConsumerRecord("topic1", partition, offset, Headers.Empty, None, "value1", 0l, 0l, 0L))
        _ <- combined.handle(ConsumerRecord("topic2", partition, offset, Headers.Empty, None, "value2", 0l, 0l, 0L))
        record1 <- records1.take
        record2 <- records2.take
      } yield (record1 must beRecordWithValue("value1")) and
        (record2 must beRecordWithValue("value2"))
    }

    "invoke both handlers for shared topics" in {
      for {
        records1 <- Queue.unbounded[ConsumerRecord[Nothing, String]]
        handler1 = RecordHandler[Any, Nothing, Nothing, String](topic)(records1.offer)

        records2 <- Queue.unbounded[ConsumerRecord[Nothing, String]]
        handler2 = RecordHandler[Any, Nothing, Nothing, String](topic)(records2.offer)

        combined = handler1 combine handler2

        _ <- combined.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, None, "value", 0l, 0l, 0L))
        record1 <- records1.take
        record2 <- records2.take
      } yield (record1 must beRecordWithValue("value")) and
        (record2 must beRecordWithValue("value"))
    }

    "not fail if no matching topic is found" in {
      val handler =
        RecordHandler[Any, Throwable, String, String]("topic1")(_ => ZIO.unit) combine
          RecordHandler[Any, Throwable, String, String]("topic2")(_ => ZIO.unit)

      val record = ConsumerRecord(topic, partition, offset, Headers.Empty, None, "value", 0l, 0l, 0L)

      handler.handle(record).either.map(_ must beRight)
    }
  }

}

object RecordHandlerTest {
  val topic = "some-topic"
  val retryTopic = s"$topic-retry"
  val group = "some-group"
  val partition = 0
  val offset = 0L
  val bytes = nextInt(9).flatMap(size => nextBytes(size + 1))

  val failingHandler = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](topic) { _ =>
    ZIO.fail(RetriableError)
  }

  val retryPolicy = FakeRetryPolicy(topic)
}
