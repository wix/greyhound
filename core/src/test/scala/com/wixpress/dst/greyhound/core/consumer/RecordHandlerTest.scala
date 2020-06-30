package com.wixpress.dst.greyhound.core.consumer

import java.time.Instant

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.BlockingState.{IgnoringAll, IgnoringOnce}
import com.wixpress.dst.greyhound.core.consumer.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.RecordHandlerTest.{partition, _}
import com.wixpress.dst.greyhound.core.consumer.RetryRecordHandlerMetric.{BlockingIgnoredForAllFor, BlockingIgnoredOnceFor, BlockingRetryOnHandlerFailed}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.FakeRetryPolicy._
import com.wixpress.dst.greyhound.core.testkit._
import org.specs2.specification.core.Fragment
import zio._
import zio.clock.Clock
import zio.duration._
import zio.random.{Random, nextBytes, nextIntBounded}
import zio.test.environment.{TestClock, TestRandom}

class RecordHandlerTest extends BaseTest[Random with Clock with TestRandom with TestClock with TestMetrics] {

  override def env =
    for {
      env <- test.environment.testEnvironment.build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  "withRetries" should {
    "produce a message to the retry topic after failure" in {
      for {
        producer <- FakeProducer.make
        topic <- randomTopicName
        retryTopic = s"$topic-retry"
        blockingState <- Ref.make[Map[TopicPartition, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(failingHandler, FakeRetryPolicy(topic), producer, Topics(Set(topic)), blockingState)
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
        topic <- randomTopicName
        retryTopic = s"$topic-retry"
        executionTime <- Promise.make[Nothing, Instant]
        handler = RecordHandler[Clock, HandlerError, Chunk[Byte], Chunk[Byte]] { _ =>
          currentTime.flatMap(executionTime.succeed)
        }
        blockingState <- Ref.make[Map[TopicPartition, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(handler, FakeRetryPolicy(topic), producer, Topics(Set(topic)), blockingState)
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

    "retry according to provided intervals" in {
      for {
        producer <- FakeProducer.make
        topic <- randomTopicName
        handleCountRef <- Ref.make(0)
        blockingState <- Ref.make[Map[TopicPartition, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(failingHandlerWith(handleCountRef),
          FakeBlockingRetryPolicy(10.millis, 500.millis), producer, Topics(Set(topic)), blockingState)
        key <- bytes
        value <- bytes
        fiber <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _ <- TestClock.adjust(1.second)
        metrics <- TestMetrics.reported
        _ <- TestClock.adjust(100.millis)
        _ <- fiber.join
        handleCount <- handleCountRef.get
      } yield {
        handleCount === 3
        metrics must contain(BlockingRetryOnHandlerFailed(TopicPartition(topic, partition), offset))
      }
    }

    Fragment.foreach(Seq(Seq(50.millis, 1.second), Seq(100.millis, 1.second), Seq(1.second, 1.second))) { retryDurations =>
      s"release blocking retry once for retry with duration ${retryDurations.map(_.toMillis)} millis" in {
        for {
          producer <- FakeProducer.make
          topic <- randomTopicName
          tpartition = TopicPartition(topic, partition)
          blockingState <- Ref.make[Map[TopicPartition, BlockingState]](Map.empty)
          retryHandler = RetryRecordHandler.withRetries(failingHandler,
            FakeBlockingRetryPolicy(retryDurations:_*), producer, Topics(Set(topic)), blockingState)
          key <- bytes
          value <- bytes
          fiber <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- TestClock.adjust(retryDurations.head.*(0.5))
          metricsBeforeReleaseOnce <- TestMetrics.reported
          _ <- blockingState.set(Map(tpartition -> IgnoringOnce))
          _ <- TestClock.adjust(retryDurations.head.*(0.6))
          _ <- fiber.join
          metricsAfterReleaseOnce <- TestMetrics.reported
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 1, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- TestClock.adjust(retryDurations.head.*(1.1))
          metricsAfterSecondHandle <- TestMetrics.reported
        } yield {
          metricsBeforeReleaseOnce must not(contain(BlockingIgnoredOnceFor(tpartition, offset)))
          metricsBeforeReleaseOnce must contain(BlockingRetryOnHandlerFailed(tpartition, offset))
          metricsAfterReleaseOnce must contain(BlockingIgnoredOnceFor(tpartition, offset))
          metricsAfterSecondHandle must contain(BlockingRetryOnHandlerFailed(tpartition, offset + 1))
          metricsAfterSecondHandle must not(contain(BlockingIgnoredOnceFor(tpartition, offset + 1)))
        }
      }
    }

    Fragment.foreach(Seq(Seq(50.millis, 1.second), Seq(100.millis, 1.minute), Seq(1.second, 1.second))) { retryDurations =>
      s"release blocking retry for all for retry with duration ${retryDurations.map(_.toMillis)} millis" in {
        for {
          producer <- FakeProducer.make
          topic <- randomTopicName
          tpartition = TopicPartition(topic, partition)
          blockingState <- Ref.make[Map[TopicPartition, BlockingState]](Map.empty)
          retryHandler = RetryRecordHandler.withRetries(failingHandler,
            FakeBlockingRetryPolicy(retryDurations:_*), producer, Topics(Set(topic)), blockingState)
          key <- bytes
          value <- bytes
          fiber <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- TestClock.adjust(retryDurations.head.*(0.5))
          metricsBeforeReleaseAll <- TestMetrics.reported
          _ <- blockingState.set(Map(tpartition -> IgnoringAll))
          _ <- TestClock.adjust(retryDurations.head.*(0.6))
          _ <- fiber.join
          metricsAfterReleaseAll <- TestMetrics.reported
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 1, Headers.Empty, Some(key), value, 0L, 0L, 0L))/*.fork*/
          metricsAfterSecondHandle <- TestMetrics.reported
        } yield {
          metricsBeforeReleaseAll must not(contain(BlockingIgnoredForAllFor(tpartition, offset)))
          metricsBeforeReleaseAll must contain(BlockingRetryOnHandlerFailed(tpartition, offset))
          metricsAfterReleaseAll must contain(BlockingIgnoredForAllFor(tpartition, offset))
          metricsAfterSecondHandle must contain(BlockingIgnoredForAllFor(tpartition, offset + 1))
        }
      }
    }
  }
}

object RecordHandlerTest {
  val group = "some-group"
  val partition = 0
  val offset = 0L
  val bytes = nextIntBounded(9).flatMap(size => nextBytes(size + 1))

  val failingHandler = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](_ => ZIO.fail(RetriableError))
  def failingHandlerWith(counter: Ref[Int]) = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](_ => counter.update(_ + 1) *> ZIO.fail(RetriableError))

  def randomAlphaChar = {
    val low = 'A'.toInt
    val high = 'z'.toInt + 1
    random.nextIntBetween(low, high).map(_.toChar)
  }

  def randomStr = ZIO.collectAll(List.fill(6)(randomAlphaChar)).map(_.mkString)
  def randomTopicName = randomStr.map(suffix => s"some-topic-$suffix")
}
