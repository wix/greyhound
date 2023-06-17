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
import com.wixpress.dst.greyhound.core.{Headers, Offset, OffsetAndMetadata, Topic, TopicPartition}
import zio._

import java.util.regex.Pattern
import zio.managed._

class EventLoopTest extends BaseTest[TestMetrics] {
  override def env: UManaged[ZEnvironment[TestMetrics]] =
    TestMetrics.makeManagedEnv

  "recover from consumer failing to poll" in {
    for {
      invocations <- Ref.make(0)
      consumer     = new EmptyConsumer {
                       override def poll(timeout: Duration)(implicit trace: Trace): Task[Records] =
                         invocations.updateAndGet(_ + 1).flatMap {
                           case 1 => ZIO.fail(exception)
                           case 2 => ZIO.succeed(recordsFrom(record))
                           case _ => ZIO.succeed(Iterable.empty)
                         }
                     }
      promise     <- Promise.make[Nothing, ConsumerRecord[Chunk[Byte], Chunk[Byte]]]
      handler      = RecordHandler(promise.succeed)
      ref         <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
      handled     <-
        ZIO.scoped(
          EventLoop
            .make("group", Topics(Set(topic)), ReportingConsumer(clientId, group, consumer), handler, "clientId", workersShutdownRef = ref)
            .flatMap(_ => promise.await)
        )
      metrics     <- TestMetrics.reported
    } yield (handled.topic, handled.offset) === (topic, offset) and (metrics must contain(PollingFailed(clientId, group, exception)))
  }

  "recover from consumer failing to commit" in {
    for {
      pollInvocations   <- Ref.make(0)
      commitInvocations <- Ref.make(0)
      promise           <- Promise.make[Nothing, Map[TopicPartition, Offset]]
      consumer           = new EmptyConsumer {
                             override def poll(timeout: Duration)(implicit trace: Trace): Task[Records] =
                               pollInvocations.updateAndGet(_ + 1).flatMap {
                                 case 1 => ZIO.succeed(recordsFrom(record))
                                 case _ => ZIO.succeed(Iterable.empty)
                               }

                             override def commit(offsets: Map[TopicPartition, Offset])(implicit trace: Trace) =
                               commitInvocations.updateAndGet(_ + 1).flatMap {
                                 case 1 => ZIO.fail(exception)
                                 case _ => promise.succeed(offsets).unit
                               }
                           }
      ref               <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
      committed         <- ZIO.scoped(
                             EventLoop
                               .make(
                                 "group",
                                 Topics(Set(topic)),
                                 ReportingConsumer(clientId, group, consumer),
                                 RecordHandler.empty,
                                 "clientId",
                                 workersShutdownRef = ref
                               )
                               .flatMap(_ => promise.await)
                           )
      metrics           <- TestMetrics.reported
      _                 <- ZIO.debug(s"metrics===$metrics")

    } yield (committed must havePair(TopicPartition(topic, partition) -> (offset + 1))) and
      (metrics must
        contain(
          CommitFailed(
            clientId,
            group,
            exception,
            Map(TopicPartition(record.topic, record.partition) -> (offset + 1))
          )
        ))
  }

//  "expose event loop health" in {
//    for {
//      _           <- ZIO.unit
//      sickConsumer = new EmptyConsumer {
//                       override def poll(timeout: Duration) (implicit trace: Trace): Task[Records] =
//                         ZIO.fail(new Exception("cough :("))
//                     }
//      ref         <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
//      died        <- ZIO.scoped(EventLoop
//                       .make("group", Topics(Set(topic)), sickConsumer, RecordHandler.empty, "clientId", workersShutdownRef = ref)
//                       .flatMap { eventLoop => eventLoop.isAlive.repeat(Schedule.spaced(10.millis) && Schedule.recurUntil[Boolean](alive => !alive)).unit }
//                       .catchAllCause(_ => ZIO.unit)
//                       .timeout(5.second))
//    } yield died must beSome
//  }

}

object EventLoopTest {
  val clientId                                         = "client-id"
  val group                                            = "group"
  val topic                                            = "topic"
  val partition                                        = 0
  val offset                                           = 0L
  val record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =
    ConsumerRecord(topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L, "")
  val exception                                        = new RuntimeException("oops")

  def recordsFrom(records: ConsumerRecord[Chunk[Byte], Chunk[Byte]]*): Records = {
    records.toIterable
  }
}

trait EmptyConsumer extends Consumer {
  override def subscribePattern[R1](topicStartsWith: Pattern, rebalanceListener: RebalanceListener[R1])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics with R1, Unit] =
    rebalanceListener.onPartitionsAssigned(this, Set(TopicPartition("", 0))).unit

  override def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics with R1, Unit] =
    rebalanceListener.onPartitionsAssigned(this, topics.map(TopicPartition(_, 0))).unit

  override def poll(timeout: Duration)(implicit trace: Trace): Task[Records] =
    ZIO.succeed(Iterable.empty)

  override def commit(offsets: Map[TopicPartition, Offset])(implicit trace: Trace): Task[Unit] =
    ZIO.unit

  override def commitWithMetadata(offsetsAndMetadata: Map[TopicPartition, OffsetAndMetadata])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics, Unit] =
    ZIO.unit

  override def commitOnRebalance(offsets: Map[TopicPartition, Offset])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics, DelayedRebalanceEffect] =
    DelayedRebalanceEffect.zioUnit

  override def commitWithMetadataOnRebalance(offsets: Map[TopicPartition, OffsetAndMetadata])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics, DelayedRebalanceEffect] =
    DelayedRebalanceEffect.zioUnit

  override def pause(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[Any, IllegalStateException, Unit] =
    ZIO.unit

  override def resume(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[Any, IllegalStateException, Unit] =
    ZIO.unit

  override def seek(partition: TopicPartition, offset: Offset)(implicit trace: Trace): ZIO[Any, IllegalStateException, Unit] =
    ZIO.unit

  override def assignment(implicit trace: Trace): Task[Set[TopicPartition]] = ZIO.succeed(Set.empty)

  override def endOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
    ZIO.succeed(Map.empty)

  override def beginningOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
    ZIO.succeed(Map.empty)

  override def position(topicPartition: TopicPartition)(implicit trace: Trace): Task[Offset] = ZIO.attempt(-1L)

  override def config(implicit trace: Trace): ConsumerConfig = ConsumerConfig("", "")

  override def offsetsForTimes(
    topicPartitionsOnTimestamp: Map[TopicPartition, Long]
  )(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] = ZIO.succeed(Map.empty)

  override def listTopics(implicit trace: Trace): RIO[Any, Map[Topic, List[core.PartitionInfo]]] = ZIO.succeed(Map.empty)

  override def committedOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
    ZIO.succeed(Map.empty)

  override def committedOffsetsAndMetadata(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, OffsetAndMetadata]] =
    ZIO.succeed(Map.empty)
}
