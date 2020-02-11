package com.wixpress.dst.greyhound.core.consumer
import com.wixpress.dst.greyhound.core.consumer.Consumer.{RebalanceListener, Records}
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.{Group, Offset, Topic}
import zio.duration.Duration
import zio.{RIO, ZIO}

case class ReportingConsumer[R](group: Group, internal: Consumer[R])
  extends Consumer[R with GreyhoundMetrics] {

  override def subscribe(topics: Set[Topic],
                         onPartitionsRevoked: RebalanceListener[R with GreyhoundMetrics],
                         onPartitionsAssigned: RebalanceListener[R with GreyhoundMetrics]): RIO[R with GreyhoundMetrics, Unit] =
    for {
      r <- ZIO.environment[R with GreyhoundMetrics]
      _ <- Metrics.report(SubscribingToTopics(group, topics))
      _ <- internal.subscribe(
        topics = topics,
        onPartitionsRevoked = { partitions =>
          (Metrics.report(PartitionsRevoked(group, partitions)) *>
            onPartitionsRevoked(partitions)).provide(r)
        },
        onPartitionsAssigned = { partitions =>
          (Metrics.report(PartitionsAssigned(group, partitions)) *>
            onPartitionsAssigned(partitions)).provide(r)
        }).tapError(error => Metrics.report(SubscribeFailed(group, error)))
    } yield ()

  override def poll(timeout: Duration): RIO[R with GreyhoundMetrics, Records] =
    internal.poll(timeout).tapError { error =>
      Metrics.report(PollingFailed(group, error))
    }

  override def commit(offsets: Map[TopicPartition, Offset]): RIO[R with GreyhoundMetrics, Unit] =
    ZIO.when(offsets.nonEmpty) {
      Metrics.report(CommittingOffsets(group, offsets)) *>
        internal.commit(offsets).tapError { error =>
          Metrics.report(CommitFailed(group, error))
        }
    }

  override def pause(partitions: Set[TopicPartition]): ZIO[R with GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      Metrics.report(PausingPartitions(group, partitions)) *>
        internal.pause(partitions).tapError { error =>
          Metrics.report(PausePartitionsFailed(group, error))
        }
    }

  override def resume(partitions: Set[TopicPartition]): ZIO[R with GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      Metrics.report(ResumingPartitions(group, partitions)) *>
        internal.resume(partitions).tapError { error =>
          Metrics.report(ResumePartitionsFailed(group, error))
        }
    }

  override def seek(partition: TopicPartition, offset: Offset): ZIO[R with GreyhoundMetrics, IllegalStateException, Unit] =
    Metrics.report(SeekingToOffset(group, partition, offset)) *>
      internal.seek(partition, offset).tapError { error =>
        Metrics.report(SeekToOffsetFailed(group, error))
      }

}

sealed trait ConsumerMetric extends GreyhoundMetric {
  def group: Group
}

object ConsumerMetric {
  case class SubscribingToTopics(group: Group, topics: Set[Topic]) extends ConsumerMetric
  case class CommittingOffsets(group: Group, offsets: Map[TopicPartition, Offset]) extends ConsumerMetric
  case class PausingPartitions(group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric
  case class ResumingPartitions(group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric
  case class SeekingToOffset(group: Group, partition: TopicPartition, offset: Offset) extends ConsumerMetric
  case class PartitionsAssigned(group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric
  case class PartitionsRevoked(group: Group, partitions: Set[TopicPartition]) extends ConsumerMetric
  case class SubscribeFailed(group: Group, error: Throwable) extends ConsumerMetric
  case class PollingFailed(group: Group, error: Throwable) extends ConsumerMetric
  case class CommitFailed(group: Group, error: Throwable) extends ConsumerMetric
  case class PausePartitionsFailed(group: Group, error: IllegalStateException) extends ConsumerMetric
  case class ResumePartitionsFailed(group: Group, error: IllegalStateException) extends ConsumerMetric
  case class SeekToOffsetFailed(group: Group, error: IllegalStateException) extends ConsumerMetric
}
