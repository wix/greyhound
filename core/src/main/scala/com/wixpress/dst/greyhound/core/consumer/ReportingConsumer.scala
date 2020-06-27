package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern

import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric._
import com.wixpress.dst.greyhound.core.consumer.ReportingConsumer.OrderedOffsets
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core._
import zio.blocking.Blocking
import zio.duration.Duration
import zio.{RIO, UIO, ZIO}

import scala.collection.JavaConverters._

case class ReportingConsumer(clientId: ClientId, group: Group, internal: Consumer)
  extends Consumer {

  override def subscribePattern[R1](pattern: Pattern, rebalanceListener: RebalanceListener[R1]): RIO[Blocking with GreyhoundMetrics with R1, Unit] =
    for {
      r <- ZIO.environment[R1 with GreyhoundMetrics]
      _ <- GreyhoundMetrics.report(SubscribingToTopicWithPattern(clientId, group, pattern.toString))
      _ <- internal.subscribePattern(pattern,
        rebalanceListener = listener(r, rebalanceListener)).tapError(error => GreyhoundMetrics.report(SubscribeFailed(clientId, group, error)))
    } yield ()


  override def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1]): RIO[Blocking with GreyhoundMetrics with R1, Unit] =
    for {
      r <- ZIO.environment[Blocking with R1 with GreyhoundMetrics]
      _ <- GreyhoundMetrics.report(SubscribingToTopics(clientId, group, topics))
      _ <- internal.subscribe(
        topics = topics,
        rebalanceListener = listener(r, rebalanceListener)).tapError(error => GreyhoundMetrics.report(SubscribeFailed(clientId, group, error)))
    } yield ()


  private def listener[R1](r: R1 with GreyhoundMetrics, rebalanceListener: RebalanceListener[R1]) = {
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(partitions: Set[TopicPartition]): UIO[Any] =
        (GreyhoundMetrics.report(PartitionsRevoked(clientId, group, partitions)) *>
          rebalanceListener.onPartitionsRevoked(partitions)).provide(r)

      override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] =
        (GreyhoundMetrics.report(PartitionsAssigned(clientId, group, partitions)) *>
          rebalanceListener.onPartitionsAssigned(partitions)).provide(r)
    }
  }

  override def poll(timeout: Duration): RIO[Blocking with GreyhoundMetrics, Records] =
    for {
      records <- internal.poll(timeout).tapError { error =>
        GreyhoundMetrics.report(PollingFailed(clientId, group, error))
      }
      _ <- GreyhoundMetrics.report(PolledRecords(clientId, group, orderedPolledRecords(records))).as(records)
    } yield records

  private def orderedPolledRecords(records: Records): OrderedOffsets = {
    val recordsPerPartition = records.partitions.asScala.map { tp => (tp, records.records(tp)) }
    val byTopic = recordsPerPartition.groupBy(_._1.topic)
    val byPartition = byTopic.map { case (topic, recordsPerPartition) =>
      (topic, recordsPerPartition.map { case (tp, records) => (tp.partition, records) })
    }

    val onlyOffsets = byPartition.mapValues(_.map { case (partition, records) => (partition, records.asScala.map(_.offset)) }
      .toSeq.sortBy(_._1)
    )
      .toSeq
      .sortBy(_._1)

    onlyOffsets
  }

  override def commit(offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean): RIO[Blocking with GreyhoundMetrics, Unit] =
    ZIO.when(offsets.nonEmpty) {
      GreyhoundMetrics.report(CommittingOffsets(clientId, group, offsets, calledOnRebalance)) *>
        internal.commit(offsets, calledOnRebalance).tapError { error =>
          GreyhoundMetrics.report(CommitFailed(clientId, group, error, offsets))
        }
    }

  override def pause(partitions: Set[TopicPartition]): ZIO[GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      GreyhoundMetrics.report(PausingPartitions(clientId, group, partitions)) *>
        internal.pause(partitions).tapError { error =>
          GreyhoundMetrics.report(PausePartitionsFailed(clientId, group, error, partitions))
        }
    }

  override def resume(partitions: Set[TopicPartition]): ZIO[GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      GreyhoundMetrics.report(ResumingPartitions(clientId, group, partitions)) *>
        internal.resume(partitions).tapError { error =>
          GreyhoundMetrics.report(ResumePartitionsFailed(clientId, group, error, partitions))
        }
    }

  override def seek(partition: TopicPartition, offset: Offset): ZIO[GreyhoundMetrics, IllegalStateException, Unit] =
    GreyhoundMetrics.report(SeekingToOffset(clientId, group, partition, offset)) *>
      internal.seek(partition, offset).tapError { error =>
        GreyhoundMetrics.report(SeekToOffsetFailed(clientId, group, error, partition, offset))
      }

}

object ReportingConsumer {
  type OrderedOffsets = Seq[(Topic, Seq[(Partition, Seq[Offset])])]
}

sealed trait ConsumerMetric extends GreyhoundMetric {
  def clientId: ClientId

  def group: Group
}

object ConsumerMetric {

  case class SubscribingToTopics(clientId: ClientId, group: Group, topics: Set[Topic]) extends ConsumerMetric

  case class SubscribingToTopicWithPattern(clientId: ClientId, group: Group, pattern: String) extends ConsumerMetric

  case class CommittingOffsets(clientId: ClientId, group: Group, offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean) extends ConsumerMetric

  case class PausingPartitions(clientId: ClientId, group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric

  case class ResumingPartitions(clientId: ClientId, group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric

  case class SeekingToOffset(clientId: ClientId, group: Group, partition: TopicPartition, offset: Offset) extends ConsumerMetric

  case class PartitionsAssigned(clientId: ClientId, group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric

  case class PartitionsRevoked(clientId: ClientId, group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric

  case class SubscribeFailed(clientId: ClientId, group: Group, error: Throwable) extends ConsumerMetric

  case class PollingFailed(clientId: ClientId, group: Group, error: Throwable) extends ConsumerMetric

  case class CommitFailed(clientId: ClientId, group: Group, error: Throwable, offsets: Map[TopicPartition, Offset]) extends ConsumerMetric

  case class PausePartitionsFailed(clientId: ClientId, group: Group, error: IllegalStateException, partitions: Set[TopicPartition]) extends ConsumerMetric

  case class ResumePartitionsFailed(clientId: ClientId, group: Group, error: IllegalStateException, partitions: Set[TopicPartition]) extends ConsumerMetric

  case class SeekToOffsetFailed(clientId: ClientId, group: Group, error: IllegalStateException, partition: TopicPartition, offset: Offset) extends ConsumerMetric

  case class PolledRecords(clientId: ClientId, group: Group, records: OrderedOffsets) extends ConsumerMetric

}
