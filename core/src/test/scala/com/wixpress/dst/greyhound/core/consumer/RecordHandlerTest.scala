package com.wixpress.dst.greyhound.core.consumer

import java.time.Instant

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
    (TestEnvironment.Value zipWith TestMetrics.make) { (env, testMetrics) =>
      new TestRandom with TestClock with TestMetrics {
        override val random: TestRandom.Service[Any] = env.random
        override val clock: TestClock.Service[Any] = env.clock
        override val scheduler: TestClock.Service[Any] = env.scheduler
        override val metrics: TestMetrics.Service = testMetrics.metrics
      }
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
        _ <- retryHandler.handle(Record(topic, partition, offset, Headers.Empty, Some(key), value))
        record <- producer.records.take
        now <- currentTime
        retryAttempt <- intSerde.serialize(retryTopic, 0)
        submittedAt <- instantSerde.serialize(retryTopic, now)
        backoff <- durationSerde.serialize(retryTopic, 1.second)
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
        key <- bytes
        value <- bytes
        result <- retryHandler.handle(Record(topic, partition, offset, Headers.Empty, Some(key), value)).flip
      } yield result must beLeft
    }

    "delay execution of user handler by configured backoff" in {
      for {
        producer <- FakeProducer.make
        executionTime <- Promise.make[Nothing, Instant]
        handler = RecordHandler[Clock, HandlerError, Chunk[Byte], Chunk[Byte]](topic) { _ =>
          currentTime.flatMap(now => executionTime.succeed(now).unit)
        }
        retryHandler = handler.withRetries(retryPolicy, producer)
        key <- bytes
        value <- bytes
        begin <- currentTime
        retryAttempt <- intSerde.serialize(retryTopic, 0)
        submittedAt <- instantSerde.serialize(retryTopic, begin)
        backoff <- durationSerde.serialize(retryTopic, 1.second)
        headers = Headers(
          "retry-attempt" -> retryAttempt,
          "retry-submitted-at" -> submittedAt,
          "retry-backoff" -> backoff)
        _ <- retryHandler.handle(Record(retryTopic, partition, offset, headers, Some(key), value)).fork
        _ <- TestClock.adjust(1.second)
        end <- executionTime.await
      } yield end must equalTo(begin.plusSeconds(1))
    }

    "fail with original error retry policy decides not to continue" in {
      for {
        producer <- FakeProducer.make
        failingHandler = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](topic)(_ => ZIO.fail(NonRetriableError))
        retryHandler = failingHandler.withRetries(retryPolicy, producer)
        key <- bytes
        value <- bytes
        result <- retryHandler.handle(Record(topic, partition, offset, Headers.Empty, Some(key), value)).flip
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
        ref1 <- Ref.make(Option.empty[String])
        handler1 = RecordHandler[Any, Nothing, Nothing, String]("topic1") { record =>
          ref1.set(Some(record.value))
        }

        ref2 <- Ref.make(Option.empty[String])
        handler2 = RecordHandler[Any, Nothing, Nothing, String]("topic2") { record =>
          ref2.set(Some(record.value))
        }

        combined = handler1 combine handler2

        _ <- combined.handle(Record("topic1", partition, offset, Headers.Empty, None, "value1"))
        _ <- combined.handle(Record("topic2", partition, offset, Headers.Empty, None, "value2"))
        value1 <- ref1.get
        value2 <- ref2.get
      } yield (value1 must beSome("value1")) and (value2 must beSome("value2"))
    }

    "invoke both handlers for shared topics" in {
      for {
        ref1 <- Ref.make(Option.empty[String])
        handler1 = RecordHandler[Any, Nothing, Nothing, String](topic) { record =>
          ref1.set(Some(record.value))
        }

        ref2 <- Ref.make(Option.empty[String])
        handler2 = RecordHandler[Any, Nothing, Nothing, String](topic) { record =>
          ref2.set(Some(record.value))
        }

        combined = handler1 combine handler2

        _ <- combined.handle(Record(topic, partition, offset, Headers.Empty, None, "value"))
        value1 <- ref1.get
        value2 <- ref2.get
      } yield (value1 must beSome("value")) and (value2 must beSome("value"))
    }

    "not fail if no matching topic is found" in {
      val handler =
        RecordHandler[Any, Throwable, String, String]("topic1")(_ => ZIO.unit) combine
          RecordHandler[Any, Throwable, String, String]("topic2")(_ => ZIO.unit)

      val record = Record(topic, partition, offset, Headers.Empty, None, "value")

      handler.handle(record).either.map(_ must beRight)
    }
  }

  "parallel" should {
    "parallelize handling based on partition" in {
      val partitions = 8

      for {
        latch <- CountDownLatch.make(partitions)
        slowHandler = RecordHandler[Clock, Nothing, Nothing, String](topic) { _ =>
          clock.sleep(1.second) *> latch.countDown
        }
        _ <- slowHandler.parallel(partitions).use { handler =>
          ZIO.foreach(0 until partitions) { partition =>
            handler.handle(Record(topic, partition, 0L, Headers.Empty, None, s"message-$partition"))
          }
        }.fork
        _ <- TestClock.adjust(1.second)
        _ <- latch.await
      } yield ok // If execution is not parallel, the latch will not be released
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
