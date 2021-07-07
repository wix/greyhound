package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern
import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.{TopicPartition, _}
import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric._
import com.wixpress.dst.greyhound.core.consumer.ReportingConsumer.OrderedOffsets
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.MetricResult
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.Duration
import zio.{RIO, Task, UIO, ZIO}


case class ReportingConsumer(clientId: ClientId, group: Group, internal: Consumer)
  extends Consumer {

  override def subscribePattern[R1](pattern: Pattern, rebalanceListener: RebalanceListener[R1]): RIO[Blocking with GreyhoundMetrics with R1, Unit] =
    for {
      r <- ZIO.environment[R1 with GreyhoundMetrics with Blocking]
      _ <- GreyhoundMetrics.report(SubscribingToTopicWithPattern(clientId, group, pattern.toString, config.consumerAttributes))
      _ <- internal.subscribePattern(pattern,
        rebalanceListener = listener(r, rebalanceListener)).tapError(error => GreyhoundMetrics.report(SubscribeFailed(clientId, group, error, config.consumerAttributes)))
    } yield ()


  override def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1]): RIO[Blocking with GreyhoundMetrics with R1, Unit] =
    for {
      r <- ZIO.environment[Blocking with R1 with GreyhoundMetrics]
      _ <- GreyhoundMetrics.report(SubscribingToTopics(clientId, group, topics, config.consumerAttributes))
      _ <- internal.subscribe(
        topics = topics,
        rebalanceListener = listener(r, rebalanceListener)).tapError(error => GreyhoundMetrics.report(SubscribeFailed(clientId, group, error, config.consumerAttributes)))
    } yield ()


  private def listener[R1](r: R1 with GreyhoundMetrics with Blocking, rebalanceListener: RebalanceListener[R1]) = {
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(partitions: Set[TopicPartition]): UIO[DelayedRebalanceEffect] =
        (GreyhoundMetrics.report(PartitionsRevoked(clientId, group, partitions, config.consumerAttributes)) *>
          rebalanceListener.onPartitionsRevoked(partitions)).provide(r)

      override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] =
        (GreyhoundMetrics.report(PartitionsAssigned(clientId, group, partitions, config.consumerAttributes)) *>
          rebalanceListener.onPartitionsAssigned(partitions)).provide(r)
    }
  }

  override def poll(timeout: Duration): RIO[Blocking with GreyhoundMetrics, Records] =
    for {
      records <- internal.poll(timeout).tapError { error =>
        GreyhoundMetrics.report(PollingFailed(clientId, group, error, config.consumerAttributes))
      }
      _ <- GreyhoundMetrics.report(PolledRecords(clientId, group, orderedPolledRecords(records), config.consumerAttributes)).as(records)
    } yield records

  private def orderedPolledRecords(records: Records): OrderedOffsets = {
    records.groupBy(_.topic).map { case (topic, records) =>
      topic -> records.groupBy(_.partition).mapValues(r => r.map(_.offset).toSeq).toSeq.sortBy(_._1)
    }.toSeq.sortBy(_._1)
  }

  override def commitOnRebalance(offsets: Map[TopicPartition, Offset]): RIO[Blocking with GreyhoundMetrics, DelayedRebalanceEffect] =
    ZIO.runtime[Blocking with GreyhoundMetrics].flatMap { runtime =>
      if (offsets.nonEmpty) {
        GreyhoundMetrics.report(CommittingOffsets(clientId, group, offsets, calledOnRebalance = true, attributes = config.consumerAttributes)) *>
          internal.commitOnRebalance(offsets).tapError { error =>
            GreyhoundMetrics.report(CommitFailed(clientId, group, error, offsets, calledOnRebalance = true, attributes = config.consumerAttributes))
          }.map(
            _.tapError { error => // handle commit errors in ThreadLockedEffect
              runtime.unsafeRunTask(GreyhoundMetrics.report(CommitFailed(clientId, group, error, offsets, calledOnRebalance = true, attributes = config.consumerAttributes)))
            } *> DelayedRebalanceEffect(runtime.unsafeRunTask(
              GreyhoundMetrics.report(CommittedOffsets(clientId, group, offsets, calledOnRebalance = true, attributes = config.consumerAttributes)
              ))))
      } else DelayedRebalanceEffect.zioUnit
    }


  override def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking with GreyhoundMetrics, Unit] = {
    ZIO.when(offsets.nonEmpty) {
      GreyhoundMetrics.report(CommittingOffsets(clientId, group, offsets, calledOnRebalance = false, attributes = config.consumerAttributes)) *>
        internal.commit(offsets).tapError { error =>
          GreyhoundMetrics.report(CommitFailed(clientId, group, error, offsets))
        } *>
        GreyhoundMetrics.report(CommittedOffsets(clientId, group, offsets, calledOnRebalance = false, attributes = config.consumerAttributes))
    }
  }

  override def pause(partitions: Set[TopicPartition]): ZIO[Blocking with GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      GreyhoundMetrics.report(PausingPartitions(clientId, group, partitions, config.consumerAttributes)) *>
        internal.pause(partitions).tapError { error =>
          GreyhoundMetrics.report(PausePartitionsFailed(clientId, group, error, partitions, config.consumerAttributes))
        }
    }

  override def resume(partitions: Set[TopicPartition]): ZIO[Blocking with GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      GreyhoundMetrics.report(ResumingPartitions(clientId, group, partitions, config.consumerAttributes)) *>
        internal.resume(partitions).tapError { error =>
          GreyhoundMetrics.report(ResumePartitionsFailed(clientId, group, error, partitions, config.consumerAttributes))
        }
    }

  override def seek(partition: TopicPartition, offset: Offset): ZIO[Blocking with GreyhoundMetrics, IllegalStateException, Unit] =
    GreyhoundMetrics.report(SeekingToOffset(clientId, group, partition, offset, config.consumerAttributes)) *>
      internal.seek(partition, offset).tapError { error =>
        GreyhoundMetrics.report(SeekToOffsetFailed(clientId, group, error, partition, offset, config.consumerAttributes))
      }

  override def assignment: Task[Set[TopicPartition]] = internal.assignment

  override def endOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]] =
    internal.endOffsets(partitions)

  override def position(topicPartition: TopicPartition): Task[Offset] =
    internal.position(topicPartition)

  override def config: ConsumerConfig = internal.config

  override def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Clock with Blocking, Map[TopicPartition, Offset]] =
    internal.offsetsForTimes(topicPartitionsOnTimestamp)

  override def listTopics: RIO[Blocking, Map[Topic, List[core.PartitionInfo]]] = internal.listTopics
}

object ReportingConsumer {
  type OrderedOffsets = Seq[(Topic, Seq[(Partition, Seq[Offset])])]
}

sealed trait ConsumerMetric extends GreyhoundMetric {
  def clientId: ClientId

  def group: Group
}

object ConsumerMetric {

  case class CreatingConsumer(clientId: ClientId, group: Group, connectUrl: String, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class SubscribingToTopics(clientId: ClientId, group: Group, topics: Set[Topic], attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class SubscribingToTopicWithPattern(clientId: ClientId, group: Group, pattern: String, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class CommittingOffsets(clientId: ClientId, group: Group, offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class CommittedOffsets(clientId: ClientId, group: Group, offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class PausingPartitions(clientId: ClientId, group: Group, partitions: Set[TopicPartition], attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class ResumingPartitions(clientId: ClientId, group: Group, partitions: Set[TopicPartition], attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class SeekingToOffset(clientId: ClientId, group: Group, partition: TopicPartition, offset: Offset, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class PartitionsAssigned(clientId: ClientId, group: Group, partitions: Set[TopicPartition], attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class PartitionsRevoked(clientId: ClientId, group: Group, partitions: Set[TopicPartition], attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class SubscribeFailed(clientId: ClientId, group: Group, error: Throwable, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class PollingFailed(clientId: ClientId, group: Group, error: Throwable, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class CommitFailed(clientId: ClientId, group: Group, error: Throwable, offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean = false, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class PausePartitionsFailed(clientId: ClientId, group: Group, error: IllegalStateException, partitions: Set[TopicPartition], attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class ResumePartitionsFailed(clientId: ClientId, group: Group, error: IllegalStateException, partitions: Set[TopicPartition], attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class SeekToOffsetFailed(clientId: ClientId, group: Group, error: IllegalStateException, partition: TopicPartition, offset: Offset, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class PolledRecords(clientId: ClientId, group: Group, records: OrderedOffsets, attributes: Map[String, String] = Map.empty) extends ConsumerMetric

  case class CommittedMissingOffsets(clientId: ClientId,
                                     group: Group,
                                     partitions: Set[TopicPartition],
                                     offsets: Map[TopicPartition, Offset],
                                     elapsed: Duration,
                                     seekTo: Map[TopicPartition, Offset]) extends ConsumerMetric

  case class CommittedMissingOffsetsFailed(clientId: ClientId,
                                           group: Group,
                                           partitions: Set[TopicPartition],
                                           offsets: Map[TopicPartition, Offset],
                                           elapsed: Duration,
                                           error: Throwable) extends ConsumerMetric

  case class RewindOffsetsOnPollError(clientId: ClientId,
                                      group: Group,
                                      positions: Map[TopicPartition, Offset],
                                      result: MetricResult[Throwable, Unit]
                                     ) extends ConsumerMetric

}
