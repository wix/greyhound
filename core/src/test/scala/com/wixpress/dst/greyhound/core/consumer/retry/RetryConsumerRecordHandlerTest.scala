package com.wixpress.dst.greyhound.core.consumer.retry

import java.time.Instant
import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{Blocked, Blocking => InternalBlocking, IgnoringAll, IgnoringOnce}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryConsumerRecordHandlerTest.{offset, partition, _}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{BlockingIgnoredForAllFor, BlockingIgnoredOnceFor, BlockingRetryHandlerInvocationFailed, NoRetryOnNonRetryableFailure}
import com.wixpress.dst.greyhound.core.producer.{ProducerError, ProducerRecord}
import com.wixpress.dst.greyhound.core.testkit.FakeRetryHelper._
import com.wixpress.dst.greyhound.core.testkit._
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown
import org.specs2.specification.core.Fragment
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.random.{nextBytes, nextIntBounded, Random}
import zio.test.environment.{TestClock, TestRandom}

class RetryConsumerRecordHandlerTest extends BaseTest[Random with Clock with Blocking with TestRandom with TestClock with TestMetrics] {

  override def env =
    for {
      env         <- test.environment.testEnvironment.build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  "withRetries" should {
    "produce a message to the retry topic after failure" in {
      for {
        producer <- FakeProducer.make
        topic    <- randomTopicName
        retryTopic = s"$topic-retry"
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandler,
          ZRetryConfig.nonBlockingRetry(1.second),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key          <- bytes
        value        <- bytes
        _            <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
        record       <- producer.records.take
        now          <- currentTime
        retryAttempt <- IntSerde.serialize(retryTopic, 0)
        submittedAt  <- InstantSerde.serialize(retryTopic, now)
        backoff      <- DurationSerde.serialize(retryTopic, 1.second)
      } yield {
        record ===
          ProducerRecord(
            retryTopic,
            value,
            Some(key),
            partition = None,
            headers = Headers("retry-attempt" -> retryAttempt, "retry-submitted-at" -> submittedAt, "retry-backoff" -> backoff)
          )
      }
    }

    "delay execution of user handler by configured backoff" in {
      for {
        producer <- FakeProducer.make
        topic    <- randomTopicName
        retryTopic = s"$topic-retry"
        executionTime <- Promise.make[Nothing, Instant]
        handler = RecordHandler[Clock, HandlerError, Chunk[Byte], Chunk[Byte]] { _ => currentTime.flatMap(executionTime.succeed) }
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          handler,
          ZRetryConfig.nonBlockingRetry(1.second),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        value        <- bytes
        begin        <- currentTime
        retryAttempt <- IntSerde.serialize(retryTopic, 0)
        submittedAt  <- InstantSerde.serialize(retryTopic, begin)
        backoff      <- DurationSerde.serialize(retryTopic, 1.second)
        headers = Headers("retry-attempt" -> retryAttempt, "retry-submitted-at" -> submittedAt, "retry-backoff" -> backoff)
        _   <- retryHandler.handle(ConsumerRecord(retryTopic, partition, offset, headers, None, value, 0L, 0L, 0L)).fork
        _   <- TestClock.adjust(1.second)
        end <- executionTime.await.disconnect.timeoutFail(TimeoutWaitingForAssertion)(5.seconds)
      } yield end must equalTo(begin.plusSeconds(1))
    }

    "retry according to provided intervals" in {
      for {
        producer <- FakeProducer.make
        topic    <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        handleCountRef <- Ref.make(0)
        blockingState  <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandlerWith(handleCountRef),
          ZRetryConfig.finiteBlockingRetry(10.millis, 500.millis),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key   <- bytes
        value <- bytes
        record = ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)
        _ <- retryHandler.handle(record).fork
        _ <- adjustTestClockFor(100.millis)
        _ <- eventuallyZ(blockingState.get)(_.get(TopicPartitionTarget(tpartition)).contains(Blocked(record)))
        _ <- adjustTestClockFor(4.seconds)
        _ <- eventuallyZ(TestClock.adjust(100.millis) *> TestMetrics.reported)(
          _.contains(BlockingRetryHandlerInvocationFailed(tpartition, offset, "RetriableError"))
        )
        _ <- adjustTestClockFor(1.second)
        _ <- eventuallyZ(handleCountRef.get)(_ == 3)
        _ <- eventuallyZ(blockingState.get)(_.get(TopicPartitionTarget(tpartition)).contains(InternalBlocking))
      } yield ok
    }

    "no retry if fails with NonRetriableError" in {
      for {
        producer <- FakeProducer.make
        topic    <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        handleCountRef <- Ref.make(0)
        blockingState  <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          nonRetryableHandlerWith(handleCountRef),
          ZRetryConfig.finiteBlockingRetry(10.millis, 500.millis),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key   <- bytes
        value <- bytes
        _     <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _     <- adjustTestClockFor(4.seconds)
        _ <- eventuallyZ(TestClock.adjust(100.millis) *> TestMetrics.reported)(
          _.contains(NoRetryOnNonRetryableFailure(tpartition, offset, cause))
        )
        _           <- adjustTestClockFor(1.second)
        handleCount <- handleCountRef.get.delay(100.milliseconds).provideSomeLayer(Clock.live)
      } yield handleCount === 1
    }

    "allow infinite retries" in {
      for {
        producer       <- FakeProducer.make
        topic          <- randomTopicName
        handleCountRef <- Ref.make(0)
        blockingState  <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandlerWith(handleCountRef),
          ZRetryConfig.infiniteBlockingRetry(100.millis),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key     <- bytes
        value   <- bytes
        _       <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _       <- adjustTestClockFor(1.second, 1.2)
        metrics <- TestMetrics.reported
        _       <- eventuallyZ(handleCountRef.get)(_ >= 10)
      } yield {
        metrics must contain(BlockingRetryHandlerInvocationFailed(TopicPartition(topic, partition), offset, "RetriableError"))
      }
    }

    Fragment.foreach(Seq(Seq(50.millis, 1.second), Seq(100.millis, 1.second), Seq(1.second, 1.second))) { retryDurations =>
      s"release blocking retry once for retry with duration ${retryDurations.map(_.toMillis)} millis" in {
        for {
          producer <- FakeProducer.make
          topic    <- randomTopicName
          tpartition = TopicPartition(topic, partition)
          blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          retryHandler = RetryRecordHandler.withRetries(
            group,
            failingHandler,
            ZRetryConfig.finiteBlockingRetry(retryDurations.head, retryDurations.drop(1): _*),
            producer,
            Topics(Set(topic)),
            blockingState,
            FakeRetryHelper(topic)
          )
          key   <- bytes
          value <- bytes
          record = ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)
          fiber <- retryHandler.handle(record).fork
          _     <- adjustTestClockFor(retryDurations.head, 0.5)
          _ <- eventuallyZ(TestMetrics.reported)(metrics =>
            !metrics.contains(BlockingIgnoredOnceFor(tpartition, offset)) &&
              metrics.contains(BlockingRetryHandlerInvocationFailed(tpartition, offset, "RetriableError"))
          )
          _ <- eventuallyZ(blockingState.get)(_.get(TopicPartitionTarget(tpartition)).contains(Blocked(record)))
          _ <- blockingState.set(Map(TopicPartitionTarget(tpartition) -> IgnoringOnce))
          _ <- adjustTestClockFor(retryDurations.head)
          _ <- fiber.join
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingIgnoredOnceFor(tpartition, offset)))
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 1, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- adjustTestClockFor(retryDurations.head, 1.5)
          _ <- eventuallyZ(TestMetrics.reported)(metrics =>
            !metrics.contains(BlockingIgnoredOnceFor(tpartition, offset + 1)) &&
              metrics.contains(BlockingRetryHandlerInvocationFailed(tpartition, offset + 1, "RetriableError"))
          )
        } yield ok
      }
    }

    s"release blocking retry once AHEAD OF TIME" in {
      for {
        producer <- FakeProducer.make
        topic    <- randomTopicName
        tpartition = TopicPartition(topic, partition)
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandler,
          ZRetryConfig.finiteBlockingRetry(50.millis, 1.second),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key   <- bytes
        value <- bytes
        _     <- blockingState.set(Map(TopicPartitionTarget(tpartition) -> IgnoringOnce))
        fiber <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _     <- adjustTestClockFor(50.millis)
        _     <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingIgnoredOnceFor(tpartition, offset)))
        _     <- fiber.join
        _     <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 1, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _     <- adjustTestClockFor(50.millis, 1.5)
        _ <- eventuallyZ(TestMetrics.reported)(metrics =>
          !metrics.contains(BlockingIgnoredOnceFor(tpartition, offset + 1)) &&
            metrics.contains(BlockingRetryHandlerInvocationFailed(tpartition, offset + 1, "RetriableError"))
        )
      } yield ok
    }

    Fragment.foreach(
      Seq(
        (Seq(50.millis, 1.second), (tpartition: TopicPartition) => TopicTarget(tpartition.topic)),
        (Seq(100.millis, 6.seconds), (tpartition: TopicPartition) => TopicTarget(tpartition.topic)),
        (Seq(1.second, 1.second), (tpartition: TopicPartition) => TopicTarget(tpartition.topic)),
        (Seq(50.millis, 1.second), (tpartition: TopicPartition) => TopicPartitionTarget(tpartition)),
        (Seq(100.millis, 3.seconds), (tpartition: TopicPartition) => TopicPartitionTarget(tpartition)),
        (Seq(1.second, 1.second), (tpartition: TopicPartition) => TopicPartitionTarget(tpartition))
      )
    ) { pair: (Seq[Duration], TopicPartition => BlockingTarget) =>
      val (retryDurations, target) = pair
      s"release blocking retry for all for ${target(TopicPartition("", 0))} for retry with duration ${retryDurations.map(_.toMillis)} millis" in {
        for {
          producer <- FakeProducer.make
          topic    <- randomTopicName
          tpartition = TopicPartition(topic, partition)
          handleCountRef <- Ref.make(0)
          blockingState  <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          retryHandler = RetryRecordHandler.withRetries(
            group,
            failingHandlerWith(handleCountRef),
            ZRetryConfig.finiteBlockingRetry(retryDurations.head, retryDurations.drop(1): _*),
            producer,
            Topics(Set(topic)),
            blockingState,
            FakeRetryHelper(topic)
          )
          key   <- bytes
          value <- bytes
          fiber <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _     <- adjustTestClockFor(retryDurations.head, 0.5)
          _ <- eventuallyZ(TestMetrics.reported)(list =>
            !list.contains(BlockingIgnoredForAllFor(tpartition, offset)) &&
              list.contains(BlockingRetryHandlerInvocationFailed(tpartition, offset, "RetriableError"))
          )
          _ <- blockingState.set(Map(target(tpartition) -> IgnoringAll))
          _ <- adjustTestClockFor(retryDurations.head)
          _ <- fiber.join
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingIgnoredForAllFor(tpartition, offset)))
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 1, Headers.Empty, Some(key), value, 0L, 0L, 0L))
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingIgnoredForAllFor(tpartition, offset + 1)))

          _ <- blockingState.set(Map(target(tpartition) -> InternalBlocking))
          _ <- handleCountRef.set(0)
          _ <- retryHandler.handle(ConsumerRecord(topic, partition, offset + 2, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
          _ <- adjustTestClockFor(retryDurations.head * 1.2)
          _ <- eventuallyZ(TestMetrics.reported)(_.contains(BlockingRetryHandlerInvocationFailed(tpartition, offset + 2, "RetriableError")))
          _ <- adjustTestClockFor(retryDurations(1) * 1.2)
          _ <- eventuallyZ(handleCountRef.get)(_ == 3)
        } yield ok
      }
    }

    "blocking then non blocking retries" in {
      for {
        producer <- FakeProducer.make
        topic    <- randomTopicName
        retryTopic = s"$topic-retry"
        tpartition = TopicPartition(topic, partition)
        handleCountRef <- Ref.make(0)
        blockingState  <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandlerWith(handleCountRef),
          ZRetryConfig.blockingFollowedByNonBlockingRetry(List(10.millis, 500.millis), NonBlockingBackoffPolicy(List(1.second))),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key   <- bytes
        value <- bytes
        _     <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).fork
        _     <- adjustTestClockFor(4.seconds)
        _ <- eventuallyZ(TestClock.adjust(100.millis) *> TestMetrics.reported)(
          _.contains(BlockingRetryHandlerInvocationFailed(tpartition, offset, "RetriableError"))
        )
        _      <- adjustTestClockFor(1.second)
        record <- producer.records.take
        _      <- eventuallyZ(handleCountRef.get)(_ == 3)
      } yield record.topic === retryTopic
    }

    "override policy for topic" in {
      for {
        producer      <- FakeProducer.make
        topic         <- randomTopicName
        otherTopic    <- randomTopicName
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        policy = ZRetryConfig.perTopicRetries {
          case `otherTopic` => RetryConfigForTopic(() => Nil, NonBlockingBackoffPolicy(1.second :: Nil))
        }
        retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandler,
          policy,
          producer,
          Topics(Set(topic, otherTopic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key    <- bytes
        value  <- bytes
        value2 <- bytes
        _      <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)) // no retry
        _ <- retryHandler.handle(ConsumerRecord(otherTopic, partition, offset, Headers.Empty, Some(key), value2, 0L, 0L, 0L)) // with retry
        producedRecords <- producer.records.takeAll
      } yield {
        producedRecords.map(_.value.get) === value2 :: Nil
      }
    }

    "on blocking retry, if failing to produce, retry until successful" in {
      val produceRetryBackoff = 100.millis
      for {
        producerFails <- Ref.make(true)
        producer <- FakeProducer.make(beforeComplete =
          r => ZIO.whenM(producerFails.get)(ZIO.fail(ProducerError.from(new RuntimeException))).as(r)
        )
        topic         <- randomTopicName
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandler,
          ZRetryConfig.nonBlockingRetry(1.second).withProduceRetryBackoff(produceRetryBackoff.asScala),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic)
        )
        key      <- bytes
        value    <- bytes
        handling <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).forkDaemon
        _        <- producer.records.takeN(3)
        _        <- producerFails.set(false)
        _        <- handling.join.withTimeout(10.seconds)
        produceAttempts <- producer.producedCount
      } yield {
        produceAttempts === 4
      }
    }.updateService[Clock.Service](_ => Clock.Service.live)

    "on consumer or worker shutdown, interrupt wait for non-blocking retry" in {
      for {
        producer      <- FakeProducer.make
        topic         <- randomTopicName
        blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
        retryHelper = alwaysBackOffRetryHelper(3.seconds)
        handling <- AwaitShutdown.makeManaged.use { awaitShutdown =>
          val retryHandler = RetryRecordHandler.withRetries(
            group,
            failingHandler,
            ZRetryConfig.nonBlockingRetry(1.second),
            producer,
            Topics(Set(topic)),
            blockingState,
            retryHelper,
            awaitShutdown = _ => UIO(awaitShutdown)
          )
          for {
            key   <- bytes
            value <- bytes
            handling <- retryHandler
              .handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L))
              .forkDaemon
          } yield handling
        }
        // we expect for the backoff sleep to be interrupted
        result <- handling.join.resurrect.either.withTimeout(10.seconds)
      } yield {
        result must beLeft(beAnInstanceOf[InterruptedException])
      }
    }
  }

  "on shutdown, interrupt wait on blocking backoff" in {
    for {
      producer       <- FakeProducer.make
      topic          <- randomTopicName
      handleCountRef <- Ref.make(0)
      blockingState  <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
      handling <- AwaitShutdown.makeManaged.use { awaitShutdown =>
        val retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandlerWith(handleCountRef),
          ZRetryConfig.finiteBlockingRetry(10.seconds),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic),
          _ => UIO(awaitShutdown.tapShutdown(_ => UIO(println("interrupting await shutdown"))))
        )
        for {
          key      <- bytes
          value    <- bytes
          handling <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).forkDaemon
        } yield handling
      }
      // we expect for the backoff sleep to be interrupted
      result        <- handling.join.resurrect.either.withTimeout(5.seconds)
      handlerCalled <- handleCountRef.get
    } yield {
      result must beLeft(beAnInstanceOf[InterruptedException])
      handlerCalled === 1
    }
  }.updateService[Clock.Service](_ => Clock.Service.live)

  "on shutdown, not retry forever on failing producer" in {
    for {
      producer      <- FakeProducer.make.map(_.failing)
      topic         <- randomTopicName
      blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
      handling <- AwaitShutdown.makeManaged.use { awaitShutdown =>
        val retryHandler = RetryRecordHandler.withRetries(
          group,
          failingHandler,
          ZRetryConfig.nonBlockingRetry(1.second),
          producer,
          Topics(Set(topic)),
          blockingState,
          FakeRetryHelper(topic),
          _ => UIO(awaitShutdown)
        )
        for {
          key      <- bytes
          value    <- bytes
          handling <- retryHandler.handle(ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)).forkDaemon
        } yield handling
      }
      // we expect produce retries to be interrupted
      result <- handling.join.resurrect.either.withTimeout(10.seconds)
    } yield {
      result must beLeft(beAnInstanceOf[InterruptedException])
    }
  }.updateService[Clock.Service](_ => Clock.Service.live)

  private def adjustTestClockFor(duration: Duration, durationMultiplier: Double = 1) = {
    val steps: Int = (10 * durationMultiplier).toInt
    ZIO.foreach_(1 to steps)(_ => TestClock.adjust(duration.*(0.1)))
  }
}

object RetryConsumerRecordHandlerTest {
  val group     = "some-group"
  val partition = 0
  val offset    = 0L
  val bytes     = nextIntBounded(9).flatMap(size => nextBytes(size + 1))

  val failingHandler = RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](_ => ZIO.fail(RetriableError))

  def failingHandlerWith(counter: Ref[Int]) =
    RecordHandler[Any, HandlerError, Chunk[Byte], Chunk[Byte]](_ => counter.update(_ + 1) *> ZIO.fail(RetriableError))

  def nonRetryableHandlerWith(counter: Ref[Int]) =
    RecordHandler[Any, Throwable, Chunk[Byte], Chunk[Byte]](_ => counter.update(_ + 1) *> ZIO.fail(NonRetriableException(cause)))

  def randomAlphaChar = {
    val low  = 'A'.toInt
    val high = 'z'.toInt + 1
    random.nextIntBetween(low, high).map(_.toChar)
  }

  def randomStr = ZIO.collectAll(List.fill(6)(randomAlphaChar)).map(_.mkString)

  def randomTopicName = randomStr.map(suffix => s"some-topic-$suffix")

  val cause = new RuntimeException("cause")

  def alwaysBackOffRetryHelper(backoff: Duration) = {
    new FakeNonBlockingRetryHelper {
      override val topic: Topic = ""
      override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]] = UIO(
        Some(RetryAttempt(topic, 1, Instant.now, backoff))
      )
    }
  }
}

object TimeoutWaitingForAssertion extends RuntimeException
