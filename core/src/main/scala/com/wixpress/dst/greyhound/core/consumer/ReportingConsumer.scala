package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern
import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.{TopicPartition, _}
import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric._
import com.wixpress.dst.greyhound.core.consumer.ReportingConsumer.OrderedOffsets
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.{report, MetricResult}
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.{Duration, RIO, Task, Trace, UIO, ZEnvironment, ZIO, ZLayer}

case class ReportingConsumer(clientId: ClientId, group: Group, internal: Consumer) extends Consumer {

  override def subscribePattern[R1](
    pattern: Pattern,
    rebalanceListener: RebalanceListener[R1]
  )(implicit trace: Trace): RIO[GreyhoundMetrics with R1, Unit] =
    for {
      r <- ZIO.environment[R1 with GreyhoundMetrics]
      _ <- report(SubscribingToTopicWithPattern(clientId, group, pattern.toString, config.consumerAttributes))
      _ <- internal
             .subscribePattern[R1](pattern, rebalanceListener = listener(r, rebalanceListener))(trace)
             .tapError(error => report(SubscribeFailed(clientId, group, error, config.consumerAttributes)))
    } yield ()

  override def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics with R1, Unit] =
    for {
      r <- ZIO.environment[R1 with GreyhoundMetrics]
      _ <- GreyhoundMetrics.report(SubscribingToTopics(clientId, group, topics, config.consumerAttributes))
      _ <- internal
             .subscribe(topics = topics, rebalanceListener = listener(r, rebalanceListener))
             .tapError(error => report(SubscribeFailed(clientId, group, error, config.consumerAttributes)))
    } yield ()

  private def listener[R1](r: ZEnvironment[R1 with GreyhoundMetrics], rebalanceListener: RebalanceListener[R1]) = {
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): UIO[DelayedRebalanceEffect] =
        (report(PartitionsRevoked(clientId, group, partitions, config.consumerAttributes)) *>
          rebalanceListener
            .onPartitionsRevoked(consumer, partitions)
            .timed
            .tap {
              case (duration, _) =>
                report(PartitionsRevokedComplete(clientId, group, partitions, config.consumerAttributes, duration.toMillis))
            }
            .map(_._2)).provideEnvironment(r)

      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(implicit trace: Trace): UIO[Any] =
        (report(PartitionsAssigned(clientId, group, partitions, config.consumerAttributes)) *>
          rebalanceListener.onPartitionsAssigned(consumer, partitions)).provideEnvironment(r)
    }
  }

  override def poll(timeout: Duration)(implicit trace: Trace): RIO[GreyhoundMetrics, Records] =
    for {
      records <- internal.poll(timeout).tapError { error => report(PollingFailed(clientId, group, error, config.consumerAttributes)) }
      _       <- report(PolledRecords(clientId, group, orderedPolledRecords(records), config.consumerAttributes)).as(records)
    } yield records

  private def orderedPolledRecords(records: Records): OrderedOffsets = {
    records
      .groupBy(_.topic)
      .map {
        case (topic, records) =>
          topic -> records.groupBy(_.partition).mapValues(r => r.map(_.offset).toSeq).toSeq.sortBy(_._1)
      }
      .toSeq
      .sortBy(_._1)
  }

  override def commitOnRebalance(
    offsets: Map[TopicPartition, Offset]
  )(implicit trace: Trace): RIO[GreyhoundMetrics, DelayedRebalanceEffect] =
    ZIO.runtime[GreyhoundMetrics].flatMap { runtime =>
      if (offsets.nonEmpty) {
        report(CommittingOffsets(clientId, group, offsets, calledOnRebalance = true, attributes = config.consumerAttributes)) *>
          internal
            .commitOnRebalance(offsets)
            .tapError { error =>
              report(CommitFailed(clientId, group, error, offsets, calledOnRebalance = true, attributes = config.consumerAttributes))
            }
            .map(
              _.tapError { error => // handle commit errors in ThreadLockedEffect
                zio.Unsafe.unsafe { implicit s =>
                  runtime.unsafe
                    .run(
                      report(
                        CommitFailed(clientId, group, error, offsets, calledOnRebalance = true, attributes = config.consumerAttributes)
                      )
                    )
                    .getOrThrowFiberFailure()
                }
              } *>
                DelayedRebalanceEffect(
                  zio.Unsafe.unsafe { implicit s =>
                    runtime.unsafe
                      .run(
                        report(CommittedOffsets(clientId, group, offsets, calledOnRebalance = true, attributes = config.consumerAttributes))
                      )
                      .getOrThrowFiberFailure()
                  }
                )
            )
      } else DelayedRebalanceEffect.zioUnit
    }

  override def commitWithMetadataOnRebalance(offsets: Map[TopicPartition, OffsetAndMetadata]
  )(implicit trace: Trace): RIO[GreyhoundMetrics, DelayedRebalanceEffect] =
    ZIO.runtime[GreyhoundMetrics].flatMap { runtime =>
      if (offsets.nonEmpty) {
        report(CommittingOffsetsWithMetadata(clientId, group, offsets, calledOnRebalance = true, attributes = config.consumerAttributes)) *>
          internal
            .commitWithMetadataOnRebalance(offsets)
            .tapError { error =>
              report(CommitWithMetadataFailed(clientId, group, error, offsets, calledOnRebalance = true, attributes = config.consumerAttributes))
            }
            .map(
              _.tapError { error => // handle commit errors in ThreadLockedEffect
                zio.Unsafe.unsafe { implicit s =>
                  runtime.unsafe
                    .run(
                      report(
                        CommitWithMetadataFailed(clientId, group, error, offsets, calledOnRebalance = true, attributes = config.consumerAttributes)
                      )
                    )
                    .getOrThrowFiberFailure()
                }
              } *>
                DelayedRebalanceEffect(
                  zio.Unsafe.unsafe { implicit s =>
                    runtime.unsafe
                      .run(
                        report(CommittedOffsetsWithMetadata(clientId, group, offsets, calledOnRebalance = true, attributes = config.consumerAttributes))
                      )
                      .getOrThrowFiberFailure()
                  }
                )
            )
      } else DelayedRebalanceEffect.zioUnit
    }

  override def commit(offsets: Map[TopicPartition, Offset])(implicit trace: Trace): RIO[GreyhoundMetrics, Unit] = {
    ZIO
      .when(offsets.nonEmpty) {
        report(
          CommittingOffsets(clientId, group, offsets, calledOnRebalance = false, attributes = config.consumerAttributes)
        ) *> internal.commit(offsets).tapError { error => report(CommitFailed(clientId, group, error, offsets)) } *>
          report(
            CommittedOffsets(clientId, group, offsets, calledOnRebalance = false, attributes = config.consumerAttributes)
          )
      }
      .unit
  }

  override def commitWithMetadata(offsetsAndMetadata: Map[TopicPartition, OffsetAndMetadata])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics, Unit] = {
    val offsets = offsetsAndMetadata.map { case (tp, om) => tp -> om.offset }
    ZIO
      .when(offsetsAndMetadata.nonEmpty) {
        report(
          CommittingOffsets(
            clientId,
            group,
            offsets,
            calledOnRebalance = false,
            attributes = config.consumerAttributes
          )
        ) *> internal.commitWithMetadata(offsetsAndMetadata).tapError { error => report(CommitFailed(clientId, group, error, offsets)) } *>
          report(
            CommittedOffsets(
              clientId,
              group,
              offsets,
              calledOnRebalance = false,
              attributes = config.consumerAttributes
            )
          )
      }
      .unit
  }

  override def pause(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO
      .when(partitions.nonEmpty) {
        report(PausingPartitions(clientId, group, partitions, config.consumerAttributes)) *>
          internal.pause(partitions).tapError { error =>
            report(PausePartitionsFailed(clientId, group, error, partitions, config.consumerAttributes))
          }
      }
      .unit

  override def resume(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO
      .when(partitions.nonEmpty) {
        report(ResumingPartitions(clientId, group, partitions, config.consumerAttributes)) *>
          internal.resume(partitions).tapError { error =>
            report(ResumePartitionsFailed(clientId, group, error, partitions, config.consumerAttributes))
          }
      }
      .unit

  override def seek(partition: TopicPartition, offset: Offset)(implicit trace: Trace): ZIO[GreyhoundMetrics, IllegalStateException, Unit] =
    report(SeekingToOffset(clientId, group, partition, offset, config.consumerAttributes)) *>
      internal.seek(partition, offset).tapError { error =>
        report(SeekToOffsetFailed(clientId, group, error, partition, offset, config.consumerAttributes))
      }

  override def assignment(implicit trace: Trace): Task[Set[TopicPartition]] = internal.assignment

  override def endOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
    internal.endOffsets(partitions)

  override def beginningOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
    internal.beginningOffsets(partitions)

  override def committedOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
    internal.committedOffsets(partitions)

  override def committedOffsetsAndMetadata(partitions: NonEmptySet[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, OffsetAndMetadata]] =
    internal.committedOffsetsAndMetadata(partitions)

  override def position(topicPartition: TopicPartition)(implicit trace: Trace): Task[Offset] =
    internal.position(topicPartition)

  override def config(implicit trace: Trace): ConsumerConfig = internal.config

  override def offsetsForTimes(
    topicPartitionsOnTimestamp: Map[TopicPartition, Long]
  )(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
    internal.offsetsForTimes(topicPartitionsOnTimestamp)

  override def listTopics(implicit trace: Trace): RIO[Any, Map[Topic, List[core.PartitionInfo]]] = internal.listTopics
}

object ReportingConsumer {
  type OrderedOffsets = Seq[(Topic, Seq[(Partition, Seq[Offset])])]
}

sealed trait ConsumerMetric extends GreyhoundMetric {
  def clientId: ClientId

  def group: Group
}

object ConsumerMetric {

  case class CreatingConsumer(clientId: ClientId, group: Group, connectUrl: String, attributes: Map[String, String] = Map.empty)
      extends ConsumerMetric

  case class SubscribingToTopics(clientId: ClientId, group: Group, topics: Set[Topic], attributes: Map[String, String] = Map.empty)
      extends ConsumerMetric

  case class SubscribingToTopicWithPattern(clientId: ClientId, group: Group, pattern: String, attributes: Map[String, String] = Map.empty)
      extends ConsumerMetric

  case class CommittingOffsets(
    clientId: ClientId,
    group: Group,
    offsets: Map[TopicPartition, Offset],
    calledOnRebalance: Boolean,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class CommittingOffsetsWithMetadata(
    clientId: ClientId,
    group: Group,
    offsets: Map[TopicPartition, OffsetAndMetadata],
    calledOnRebalance: Boolean,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class CommittedOffsets(
    clientId: ClientId,
    group: Group,
    offsets: Map[TopicPartition, Offset],
    calledOnRebalance: Boolean,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class CommittedOffsetsWithMetadata(
    clientId: ClientId,
    group: Group,
    offsets: Map[TopicPartition, OffsetAndMetadata],
    calledOnRebalance: Boolean,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class PausingPartitions(
    clientId: ClientId,
    group: Group,
    partitions: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class ResumingPartitions(
    clientId: ClientId,
    group: Group,
    partitions: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class SeekingToOffset(
    clientId: ClientId,
    group: Group,
    partition: TopicPartition,
    offset: Offset,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class PartitionsAssigned(
    clientId: ClientId,
    group: Group,
    partitions: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class PartitionsRevoked(
    clientId: ClientId,
    group: Group,
    partitions: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class PartitionsRevokedComplete(
    clientId: ClientId,
    group: Group,
    partitions: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty,
    durationMs: Long
  ) extends ConsumerMetric

  case class SubscribeFailed(clientId: ClientId, group: Group, error: Throwable, attributes: Map[String, String] = Map.empty)
      extends ConsumerMetric

  case class PollingFailed(clientId: ClientId, group: Group, error: Throwable, attributes: Map[String, String] = Map.empty)
      extends ConsumerMetric

  case class CommitFailed(
    clientId: ClientId,
    group: Group,
    error: Throwable,
    offsets: Map[TopicPartition, Offset],
    calledOnRebalance: Boolean = false,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class CommitWithMetadataFailed(
    clientId: ClientId,
    group: Group,
    error: Throwable,
    offsets: Map[TopicPartition, OffsetAndMetadata],
    calledOnRebalance: Boolean = false,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class PausePartitionsFailed(
    clientId: ClientId,
    group: Group,
    error: IllegalStateException,
    partitions: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class ResumePartitionsFailed(
    clientId: ClientId,
    group: Group,
    error: IllegalStateException,
    partitions: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class SeekToOffsetFailed(
    clientId: ClientId,
    group: Group,
    error: IllegalStateException,
    partition: TopicPartition,
    offset: Offset,
    attributes: Map[String, String] = Map.empty
  ) extends ConsumerMetric

  case class PolledRecords(clientId: ClientId, group: Group, records: OrderedOffsets, attributes: Map[String, String] = Map.empty)
      extends ConsumerMetric

  case class CommittedMissingOffsets(
    clientId: ClientId,
    group: Group,
    partitions: Set[TopicPartition],
    offsets: Map[TopicPartition, Offset],
    elapsed: Duration,
    seekTo: Map[TopicPartition, Offset]
  ) extends ConsumerMetric

  case class CommittedMissingOffsetsFailed(
    clientId: ClientId,
    group: Group,
    partitions: Set[TopicPartition],
    offsets: Map[TopicPartition, Offset],
    elapsed: Duration,
    error: Throwable
  ) extends ConsumerMetric

  case class RewindOffsetsOnPollError(
    clientId: ClientId,
    group: Group,
    positions: Map[TopicPartition, Offset],
    result: MetricResult[Throwable, Unit]
  ) extends ConsumerMetric

  case class ClosedConsumer(group: Group, clientId: ClientId, result: MetricResult[Throwable, Unit]) extends ConsumerMetric

  case class SkippedGapsOnInitialization(
    clientId: ClientId,
    group: Group,
    skippedGaps: Map[TopicPartition, OffsetAndGaps],
    currentCommittedOffsetsAndGaps: Map[TopicPartition, OffsetAndGaps]
  ) extends ConsumerMetric

  case class FoundGapsOnInitialization(
    clientId: ClientId,
    group: Group,
    gapsSmallestOffsets: Map[TopicPartition, OffsetAndMetadata]
  ) extends ConsumerMetric

}
