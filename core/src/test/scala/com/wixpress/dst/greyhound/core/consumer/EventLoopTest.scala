package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric._
import com.wixpress.dst.greyhound.core.consumer.EventLoopTest._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, TestMetrics}
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown.ShutdownPromise
import com.wixpress.dst.greyhound.core.{Headers, Offset, Topic, TopicPartition}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import java.util.regex.Pattern

class EventLoopTest extends BaseTest[Blocking with ZEnv with TestMetrics] {

  override def env: UManaged[ZEnv with TestMetrics] =
    for {
      env <- test.environment.liveEnvironment.build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  "recover from consumer failing to poll" in {
    for {
      invocations <- Ref.make(0)
      consumer = new EmptyConsumer {
        override def poll(timeout: Duration): Task[Records] =
          invocations.updateAndGet(_ + 1).flatMap {
            case 1 => ZIO.fail(exception)
            case 2 => ZIO.succeed(recordsFrom(record))
            case _ => ZIO.succeed(Iterable.empty)
          }
      }
      promise <- Promise.make[Nothing, ConsumerRecord[Chunk[Byte], Chunk[Byte]]]
      handler = RecordHandler(promise.succeed)
      ref <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
      handled <- EventLoop.make("group", Topics(Set(topic)), ReportingConsumer(clientId, group, consumer),
        handler, "clientId", workersShutdownRef = ref).use_(promise.await)
      metrics <- TestMetrics.reported
    } yield (handled.topic, handled.offset) === (topic, offset) and (metrics must contain(PollingFailed(clientId, group, exception)))
  }

  "recover from consumer failing to commit" in {
    for {
      pollInvocations <- Ref.make(0)
      commitInvocations <- Ref.make(0)
      promise <- Promise.make[Nothing, Map[TopicPartition, Offset]]
      consumer = new EmptyConsumer {
        override def poll(timeout: Duration): Task[Records] =
          pollInvocations.updateAndGet(_ + 1).flatMap {
            case 1 => ZIO.succeed(recordsFrom(record))
            case _ => ZIO.succeed(Iterable.empty)
          }

        override def commit(offsets: Map[TopicPartition, Offset]) =
          commitInvocations.updateAndGet(_ + 1).flatMap {
            case 1 => ZIO.fail(exception)
            case _ => promise.succeed(offsets).unit
          }
      }
      ref <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
      committed <- EventLoop.make("group", Topics(Set(topic)), ReportingConsumer(clientId, group, consumer),
        RecordHandler.empty, "clientId", workersShutdownRef = ref).use_(promise.await)
      metrics <- TestMetrics.reported
    } yield (committed must havePair(TopicPartition(topic, partition) -> (offset + 1))) and
      (metrics must contain(CommitFailed(clientId, group, exception, Map(TopicPartition(record.topic, record.partition) -> (offset + 1)))))
  }

  "expose event loop health" in {
    for {
      _ <- ZIO.unit
      sickConsumer = new EmptyConsumer {
        override def poll(timeout: Duration): Task[Records] =
          ZIO.dieMessage("cough :(")
      }
      ref <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
      died <- EventLoop.make("group", Topics(Set(topic)), sickConsumer, RecordHandler.empty, "clientId",
        workersShutdownRef = ref).use { eventLoop =>
        eventLoop.isAlive.repeat(Schedule.spaced(10.millis) && Schedule.recurUntil(alive => !alive)).unit
      }.catchAllCause(_ => ZIO.unit).timeout(5.second)
    } yield died must beSome
  }

}

object EventLoopTest {
  val clientId = "client-id"
  val group = "group"
  val topic = "topic"
  val partition = 0
  val offset = 0L
  val record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] = ConsumerRecord(topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
  val exception = new RuntimeException("oops")

  def recordsFrom(records: ConsumerRecord[Chunk[Byte], Chunk[Byte]]*): Records = {
    records.toIterable
  }
}

trait EmptyConsumer extends Consumer {

  override def subscribePattern[R1](pattern: Pattern, rebalanceListener: RebalanceListener[R1]): RIO[R1, Unit] =
    rebalanceListener.onPartitionsAssigned(Set(TopicPartition("", 0))).unit

  override def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1]): RIO[R1, Unit] =
    rebalanceListener.onPartitionsAssigned(topics.map(TopicPartition(_, 0))).unit

  override def poll(timeout: Duration): Task[Records] =
    ZIO.succeed(Iterable.empty)

  override def commit(offsets: Map[TopicPartition, Offset]): Task[Unit] =
    ZIO.unit

  override def commitOnRebalance(offsets: Map[TopicPartition, Offset]): RIO[Blocking with GreyhoundMetrics, DelayedRebalanceEffect] =
    DelayedRebalanceEffect.zioUnit

  override def pause(partitions: Set[TopicPartition]): ZIO[Any, IllegalStateException, Unit] =
    ZIO.unit

  override def resume(partitions: Set[TopicPartition]): ZIO[Any, IllegalStateException, Unit] =
    ZIO.unit

  override def seek(partition: TopicPartition, offset: Offset): ZIO[Any, IllegalStateException, Unit] =
    ZIO.unit

  override def assignment: Task[Set[TopicPartition]] = UIO(Set.empty)

  override def endOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]] = ZIO(Map.empty)

  override def position(topicPartition: TopicPartition): Task[Offset] = Task(-1L)

  override def config: ConsumerConfig = ConsumerConfig("", "")

  override def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Clock with Blocking, Map[TopicPartition, Offset]] = ZIO(Map.empty)

  override def listTopics: RIO[Blocking, Map[Topic, List[core.PartitionInfo]]] = UIO(Map.empty)
}
