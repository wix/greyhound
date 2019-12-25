package com.wixpress.dst.greyhound.core.consumer

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.ConsumerSpecTest._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, FakeProducer, TestMetrics}
import org.apache.kafka.common.serialization.StringDeserializer
import zio._
import zio.clock.Clock
import zio.duration._
import zio.random.{nextInt, nextString}
import zio.test.environment.{TestClock, TestEnvironment, TestRandom}

class ConsumerSpecTest extends BaseTest[TestRandom with TestClock with TestMetrics] {

  override def env: Managed[Nothing, TestRandom with TestClock with TestMetrics] =
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
      for {
        producer <- FakeProducer.make
        spec <- ConsumerSpec.withRetries(
          topic = Topic(topic),
          group = group,
          handler = RecordHandler[Any, Nothing, String, String](_ => ZIO.unit),
          keyDeserializer = deserializer,
          valueDeserializer = deserializer,
          backoffs = Vector(10.seconds, 1.minute),
          producer = producer)
      } yield spec.topics must equalTo(Set(topic, s"$topic-$group-retry-0", s"$topic-$group-retry-1"))
    }

    "produce a message to the retry topic after first failure" in {
      for {
        producer <- FakeProducer.make
        spec <- ConsumerSpec.withRetries(
          topic = Topic(topic),
          group = group,
          handler = failingHandler,
          keyDeserializer = deserializer,
          valueDeserializer = deserializer,
          backoffs = Vector(1.second),
          producer = producer)
        key <- bytes
        value <- bytes
        headers = Headers.from("foo" -> "bar")
        _ <- spec.handler.handle(Record(topic, partition, offset, headers, Some(key), value))
        record <- producer.records.take
        now <- currentTime
      } yield record must equalTo(
        ProducerRecord(
          topic = Topic(s"$topic-$group-retry-0"),
          value = value,
          key = Some(key),
          partition = None,
          headers = headers +
            ("submitTimestamp" -> Chunk.fromArray(now.toEpochMilli.toString.getBytes)) +
            ("backOffTimeMs" -> Chunk.fromArray("1000".getBytes))))
    }

    "produce a message to the retry topic after subsequent failure" in {
      for {
        producer <- FakeProducer.make
        spec <- ConsumerSpec.withRetries(
          topic = Topic(topic),
          group = group,
          handler = failingHandler,
          keyDeserializer = deserializer,
          valueDeserializer = deserializer,
          backoffs = Vector(10.seconds, 1.minute),
          producer = producer)
        key <- bytes
        value <- bytes
        now <- currentTime
        headers = Headers.from("submitTimestamp" -> now.toEpochMilli.toString, "backOffTimeMs" -> "10000")
        _ <- spec.handler.handle(Record(retryTopic, partition, offset, headers, Some(key), value)).fork
        _ <- TestClock.adjust(10.seconds)
        record <- producer.records.take
      } yield record must equalTo(
        ProducerRecord(
          topic = Topic(s"$topic-$group-retry-1"),
          value = value,
          key = Some(key),
          partition = None,
          headers = headers +
            ("submitTimestamp" -> Chunk.fromArray(now.plusSeconds(10).toEpochMilli.toString.getBytes)) +
            ("backOffTimeMs" -> Chunk.fromArray("60000".getBytes))))
    }

    "delay execution of user handler by configured backoff" in {
      for {
        producer <- FakeProducer.make
        executionTime <- Promise.make[Nothing, Instant]
        spec <- ConsumerSpec.withRetries(
          topic = Topic(topic),
          group = group,
          handler = RecordHandler[Clock, Nothing, String, String] { _ =>
            currentTime.flatMap { now =>
              executionTime.succeed(now).unit
            }
          },
          keyDeserializer = deserializer,
          valueDeserializer = deserializer,
          backoffs = Vector(3.seconds),
          producer = producer)
        key <- bytes
        value <- bytes
        begin <- currentTime
        headers = Headers.from("submitTimestamp" -> begin.toEpochMilli.toString, "backOffTimeMs" -> "3000")
        _ <- spec.handler.handle(Record(retryTopic, partition, offset, headers, Some(key), value)).fork
        _ <- TestClock.adjust(3.seconds)
        end <- executionTime.await
      } yield end must equalTo(begin.plusSeconds(3))
    }

    "fail with original error if no more retry attempts remain" in {
      for {
        producer <- FakeProducer.make
        spec <- ConsumerSpec.withRetries(
          topic = Topic(topic),
          group = group,
          handler = failingHandler,
          keyDeserializer = deserializer,
          valueDeserializer = deserializer,
          backoffs = Vector(10.seconds),
          producer = producer)
        key <- bytes
        value <- bytes
        now <- currentTime
        headers = Headers.from("submitTimestamp" -> now.toEpochMilli.toString, "backOffTimeMs" -> "10000")
        _ <- spec.handler.handle(Record(retryTopic, partition, offset, headers, Some(key), value)).fork
        _ <- TestClock.adjust(10.seconds)
        metric <- TestMetrics.queue.flatMap(_.take)
      } yield metric must equalTo(RetryUserError(error))
    }

//    "report error information when " in {
//      for {
//        producer <- FakeProducer.make
//        spec <- ConsumerSpec.withRetries(
//          topic = Topic(topic),
//          group = group,
//          handler = failingHandler,
//          keyDeserializer = deserializer,
//          valueDeserializer = deserializer,
//          backoffs = Vector(1.second),
//          producer = producer)
//        key <- bytes
//        value <- bytes
//        _ <- spec.handler.handle(Record(topic, partition, offset, Headers.Empty, Some(key), value))
//        metrics <- TestMetrics.reported
//      } yield metrics must contain(RetryUserError(error))
//    }
  }

//  "BackoffHandler" should {
//    "not sleep if retry attempt is empty" in {
//      for {
//        start <- currentTime
//        _ <- BackoffHandler[String, String].handle {
//          Record(topic, partition, offset, Headers.Empty, None, (None, ""))
//        }
//        end <- currentTime
//      } yield start must equalTo(end)
//    }
//
//    "backoff with correct time" in {
//      for {
//        start <- currentTime
//        fiber <- BackoffHandler[String, String].handle {
//          val attempt = RetryAttempt(0, start, 1.second)
//          Record(topic, partition, offset, Headers.Empty, None, (Some(attempt), ""))
//        }.fork
//        _ <- TestClock.adjust(1.second)
//        _ <- fiber.join
//        end <- currentTime
//      } yield end must equalTo(start.plusSeconds(1))
//    }
//
//    "backoff with correct time when current time is different than submission time" in {
//      for {
//        start <- currentTime
//        fiber <- BackoffHandler[String, String].handle {
//          val attempt = RetryAttempt(0, start.minusSeconds(1), 3.seconds)
//          Record(topic, partition, offset, Headers.Empty, None, (Some(attempt), ""))
//        }.fork
//        _ <- TestClock.adjust(2.seconds)
//        _ <- fiber.join
//        end <- currentTime
//      } yield end must equalTo(start.plusSeconds(2))
//    }
//
//    "not sleep if backoff time expired" in {
//      for {
//        start <- currentTime
//        _ <- BackoffHandler[String, String].handle {
//          val attempt = RetryAttempt(0, start.minusSeconds(3), 1.second)
//          Record(topic, partition, offset, Headers.Empty, None, (Some(attempt), ""))
//        }
//        end <- currentTime
//      } yield start must equalTo(end)
//    }
//  }

}

object ConsumerSpecTest {
  val topic = "some-topic"
  val group = "some-group"
  val retryTopic = s"$topic-$group-retry-0"
  val partition = 0
  val offset = 0L
  val bytes = for {
    size <- nextInt(9)
    string <- nextString(size + 1)
  } yield Chunk.fromArray(string.getBytes(UTF_8))
  val deserializer = Deserializer(new StringDeserializer)
  val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)
  val error = "Oops"
  val failingHandler: RecordHandler[Any, String, String, String] =
    RecordHandler(_ => ZIO.fail(error))
}
