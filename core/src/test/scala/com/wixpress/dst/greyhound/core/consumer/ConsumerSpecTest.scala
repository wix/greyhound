package com.wixpress.dst.greyhound.core.consumer

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerError, ProducerRecord, RecordMetadata}
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, TestMetrics}
import org.apache.kafka.common.serialization.Serdes.StringSerde
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

  val topic = "some-topic"
  val group = "some-group"
  val retryTopic = s"$topic-$group-retry-0"
  val partition = 0
  val offset = 0L
  val bytes = for {
    size <- nextInt(9)
    string <- nextString(size + 1)
  } yield Chunk.fromArray(string.getBytes(UTF_8))
  val serde = Serde(new StringSerde)
  val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)
  val failingHandler: RecordHandler[Any, String, String, String] =
    RecordHandler(_ => ZIO.fail("Oops"))

  "withRetries" should {
    "add retry topics" in {
      for {
        producer <- FakeProducer.make
        spec <- ConsumerSpec.withRetries(
          topic = Topic(topic),
          group = group,
          handler = RecordHandler[Any, Nothing, String, String](_ => ZIO.unit),
          keySerde = serde,
          valueSerde = serde,
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
          keySerde = serde,
          valueSerde = serde,
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
          keySerde = serde,
          valueSerde = serde,
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
          keySerde = serde,
          valueSerde = serde,
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
          keySerde = serde,
          valueSerde = serde,
          backoffs = Vector(10.seconds),
          producer = producer)
        key <- bytes
        value <- bytes
        now <- currentTime
        headers = Headers.from("submitTimestamp" -> now.toEpochMilli.toString, "backOffTimeMs" -> "10000")
        _ <- spec.handler.handle(Record(retryTopic, partition, offset, headers, Some(key), value)).fork
        _ <- TestClock.adjust(10.seconds)
        metric <- ZIO.accessM[TestMetrics](_.metrics.queue.take)
      } yield metric must equalTo(RetryUserError("Oops"))
    }
  }

}

case class FakeProducer(records: Queue[ProducerRecord[Chunk[Byte], Chunk[Byte]]]) extends Producer {

  override def produce[K, V](record: ProducerRecord[K, V],
                             keySerializer: Serializer[K],
                             valueSerializer: Serializer[V]): IO[ProducerError, RecordMetadata] =
    (for {
      keyBytes <- ZIO.foreach(record.key)(keySerializer.serialize(record.topic.name, _))
      valueBytes <- valueSerializer.serialize(record.topic.name, record.value)
      newRecord = ProducerRecord(
        topic = Topic(record.topic.name),
        value = valueBytes,
        key = keyBytes.headOption,
        partition = record.partition,
        headers = record.headers)
      _ <- records.offer(newRecord)
    } yield RecordMetadata(record.topic.name, 0, 0)).orDie

}

object FakeProducer {
  def make: UIO[FakeProducer] =
    Queue.unbounded[ProducerRecord[Chunk[Byte], Chunk[Byte]]].map(FakeProducer(_))
}
