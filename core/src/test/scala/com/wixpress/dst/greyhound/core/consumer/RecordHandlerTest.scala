package com.wixpress.dst.greyhound.core.consumer

import java.time.Instant

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RecordHandlerTest.{offset, partition, _}
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{IgnoringAll, IgnoringOnce, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{BlockingIgnoredForAllFor, BlockingIgnoredOnceFor, BlockingRetryOnHandlerFailed}
import com.wixpress.dst.greyhound.core.consumer.retry.{BlockingState, BlockingTarget, TopicPartitionTarget, TopicTarget, ZRetryConfig}
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
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(failingHandler, ZRetryConfig.nonBlockingRetry(1.second), producer, Topics(Set(topic)), blockingState, FakeRetryPolicy(topic))
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
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(handler, ZRetryConfig.nonBlockingRetry(1.second), producer, Topics(Set(topic)), blockingState, FakeRetryPolicy(topic))
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
        tpartition = TopicPartition(topic, partition)
        handleCountRef <- Ref.make(0)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(failingHandlerWith(handleCountRef),
          ZRetryConfig.finiteBlockingRetry(10.millis, 500.millis), producer, Topics(Set(topic)), blockingState, FakeRetryPolicy(topic))
        key <- bytes
        value <- bytes
        _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _ <- adjustTestClockFor(4.seconds)
        _ <- eventuallyZ(TestClock.adjust(100.millis) *> TestMetrics.reported)(_.contains(BlockingRetryOnHandlerFailed(tpartition, offset)))
        _ <- adjustTestClockFor(1.second)
        _ <- eventuallyZ(handleCountRef.get)(_ == 3)
      } yield ok
    }

    "allow infinite retries" in {
      for {
        producer <- FakeProducer.make
        topic <- randomTopicName
        handleCountRef <- Ref.make(0)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(failingHandlerWith(handleCountRef),
          ZRetryConfig.infiniteBlockingRetry(100.millis), producer, Topics(Set(topic)), blockingState, FakeRetryPolicy(topic))
        key <- bytes
        value <- bytes
        _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _ <- adjustTestClockFor(1.second)
        metrics <- TestMetrics.reported
        _ <- eventuallyZ(handleCountRef.get)(_ >= 10)
      } yield {
        metrics must contain(BlockingRetryOnHandlerFailed(TopicPartition(topic, partition), offset))
      }
    }

    Fragment.foreach(Seq(Seq(50.millis, 1.second), Seq(100.millis, 1.second), Seq(1.second, 1.second))) { retryDurations =>
      s"release blocking retry once for retry with duration ${retryDurations.map(_.toMillis)} millis" in {
        for {
          producer <- FakeProducer.make
          topic <- randomTopicName
          tpartition = TopicPartition(topic, partition)
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          retryHandler = RetryRecordHandler.withRetries(failingHandler,
            ZRetryConfig.finiteBlockingRetry(retryDurations.head, retryDurations.drop(1):_*), producer, Topics(Set(topic)), blockingState, FakeRetryPolicy(topic))
          key <- bytes
          value <- bytes
          fiber <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- adjustTestClockFor(retryDurations.head, 0.5)
          _ <- eventuallyZ(TestMetrics.reported)(metrics =>
            !metrics.contains(BlockingIgnoredOnceFor(tpartition, offset)) && metrics.contains(BlockingRetryOnHandlerFailed(tpartition, offset)))
          _ <- blockingState.set(Map(TopicPartitionTarget(tpartition) -> IgnoringOnce))
          _ <- adjustTestClockFor(retryDurations.head)
          _ <- fiber.join
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingIgnoredOnceFor(tpartition, offset)))
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 1, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- adjustTestClockFor(retryDurations.head, 1.5)
          _ <- eventuallyZ(TestMetrics.reported)(metrics =>
            !metrics.contains(BlockingIgnoredOnceFor(tpartition, offset + 1)) && metrics.contains(BlockingRetryOnHandlerFailed(tpartition, offset + 1)))
        } yield ok
      }
    }

    Fragment.foreach(Seq(
      (Seq(50.millis, 1.second), (tpartition:TopicPartition) => TopicTarget(tpartition.topic)),
      (Seq(100.millis, 1.minute), (tpartition:TopicPartition) => TopicTarget(tpartition.topic)),
      (Seq(1.second, 1.second), (tpartition:TopicPartition) => TopicTarget(tpartition.topic)),
      (Seq(50.millis, 1.second), (tpartition:TopicPartition) => TopicPartitionTarget(tpartition)),
      (Seq(100.millis, 1.minute), (tpartition:TopicPartition) => TopicPartitionTarget(tpartition)),
      (Seq(1.second, 1.second), (tpartition:TopicPartition) => TopicPartitionTarget(tpartition)))) { pair: (Seq[Duration], TopicPartition => BlockingTarget) =>
      val (retryDurations, target ) = pair
      s"release blocking retry for all for ${target(TopicPartition("", 0))} for retry with duration ${retryDurations.map(_.toMillis)} millis" in {
        for {
          producer <- FakeProducer.make
          topic <- randomTopicName
          tpartition = TopicPartition(topic, partition)
          handleCountRef <- Ref.make(0)
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          retryHandler = RetryRecordHandler.withRetries(failingHandlerWith(handleCountRef),
            ZRetryConfig.finiteBlockingRetry(retryDurations.head, retryDurations.drop(1):_*), producer, Topics(Set(topic)), blockingState, FakeRetryPolicy(topic))
          key <- bytes
          value <- bytes
          fiber <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- adjustTestClockFor(retryDurations.head, 0.5)
          _ <- eventuallyZ(TestMetrics.reported)(list => !list.contains(BlockingIgnoredForAllFor(tpartition, offset)) && list.contains(BlockingRetryOnHandlerFailed(tpartition, offset)))
          _ <- blockingState.set(Map(target(tpartition) -> IgnoringAll))
          _ <- adjustTestClockFor(retryDurations.head)
          _ <- fiber.join
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingIgnoredForAllFor(tpartition, offset)))
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 1, Headers.Empty, Some(key), value, 0L, 0L, 0L))
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingIgnoredForAllFor(tpartition, offset + 1)))

          _ <- blockingState.set(Map(target(tpartition) -> InternalBlocking))
          _ <- handleCountRef.set(0)
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 2, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- adjustTestClockFor(retryDurations.head)
          _ <- adjustTestClockFor(retryDurations(1))
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingRetryOnHandlerFailed(tpartition, offset + 2)))
          _ <- eventuallyZ(handleCountRef.get)(_ == 3)
        } yield ok
      }
    }

    "blocking then non blocking retries" in {
      for {
        producer <- FakeProducer.make
        topic <- randomTopicName
        retryTopic = s"$topic-retry"
        tpartition = TopicPartition(topic, partition)
        handleCountRef <- Ref.make(0)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(failingHandlerWith(handleCountRef),
          ZRetryConfig.blockingFollowedByNonBlockingRetry(List(10.millis, 500.millis), List(1.second)), producer, Topics(Set(topic)), blockingState, FakeRetryPolicy(topic))
        key <- bytes
        value <- bytes
        _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _ <- adjustTestClockFor(4.seconds)
        _ <- eventuallyZ(TestClock.adjust(100.millis) *> TestMetrics.reported)(_.contains(BlockingRetryOnHandlerFailed(tpartition, offset)))
        _ <- adjustTestClockFor(1.second)
        record <- producer.records.take
        _ <- eventuallyZ(handleCountRef.get)(_ == 3)
      } yield record.topic === retryTopic
    }
  }

  private def adjustTestClockFor(duration: Duration, durationMultiplier: Double = 1) = {
    val steps: Int =(10 * durationMultiplier).toInt
    ZIO.foreach_(1 to steps)(_ => TestClock.adjust(duration.*(0.1)))
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
