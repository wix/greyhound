package com.wixpress.dst.greyhound.core.consumer
import com.wixpress.dst.greyhound.core.consumer.Consumer.{RebalanceListener, Records}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.{Offset, Topic}
import zio.duration.Duration
import zio.{RIO, ZIO}

// TODO test?
case class ReportingConsumer[R](internal: Consumer[R])
  extends Consumer[R with GreyhoundMetrics] {

  override def subscribe(topics: Set[Topic],
                         onPartitionsRevoked: RebalanceListener[R with GreyhoundMetrics],
                         onPartitionsAssigned: RebalanceListener[R with GreyhoundMetrics]): RIO[R with GreyhoundMetrics, Unit] =
    for {
      r <- ZIO.environment[R with GreyhoundMetrics]
      _ <- Metrics.report(SubscribingToTopics(topics))
      _ <- internal.subscribe(
        topics = topics,
        onPartitionsRevoked = { partitions =>
          (Metrics.report(PartitionsRevoked(partitions)) *>
            onPartitionsRevoked(partitions)).provide(r)
        },
        onPartitionsAssigned = { partitions =>
          (Metrics.report(PartitionsAssigned(partitions)) *>
            onPartitionsAssigned(partitions)).provide(r)
        })
    } yield ()

  override def poll(timeout: Duration): RIO[R with GreyhoundMetrics, Records] =
    internal.poll(timeout).tapError { error =>
      Metrics.report(PollingFailed(error))
    }

  override def commit(offsets: Map[TopicPartition, Offset]): RIO[R with GreyhoundMetrics, Unit] =
    ZIO.when(offsets.nonEmpty) {
      Metrics.report(CommittingOffsets(offsets)) *>
        internal.commit(offsets).tapError { error =>
          Metrics.report(CommitFailed(error))
        }
    }

  override def pause(partitions: Set[TopicPartition]): ZIO[R with GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      Metrics.report(PausingPartitions(partitions)) *>
        internal.pause(partitions).tapError { error =>
          Metrics.report(PausePartitionsFailed(error))
        }
    }

  override def resume(partitions: Set[TopicPartition]): ZIO[R with GreyhoundMetrics, IllegalStateException, Unit] =
    ZIO.when(partitions.nonEmpty) {
      Metrics.report(ResumingPartitions(partitions)) *>
        internal.resume(partitions).tapError { error =>
          Metrics.report(ResumePartitionsFailed(error))
        }
    }

  override def seek(partition: TopicPartition, offset: Offset): ZIO[R with GreyhoundMetrics, IllegalStateException, Unit] =
    Metrics.report(SeekingToOffset(partition, offset)) *>
      internal.seek(partition, offset).tapError { error =>
        Metrics.report(SeekToOffsetFailed(error))
      }

}

sealed trait ConsumerMetric extends GreyhoundMetric
case class SubscribingToTopics(topics: Set[Topic]) extends ConsumerMetric
case class CommittingOffsets(offsets: Map[TopicPartition, Offset]) extends ConsumerMetric
case class PausingPartitions(partitions: Set[TopicPartition]) extends ConsumerMetric
case class ResumingPartitions(partitions: Set[TopicPartition]) extends ConsumerMetric
case class SeekingToOffset(partition: TopicPartition, offset: Offset) extends ConsumerMetric
case class PartitionsAssigned(partitions: Set[TopicPartition]) extends ConsumerMetric
case class PartitionsRevoked(partitions: Set[TopicPartition]) extends ConsumerMetric
case class PollingFailed(error: Throwable) extends ConsumerMetric
case class CommitFailed(error: Throwable) extends ConsumerMetric
case class PausePartitionsFailed(error: IllegalStateException) extends ConsumerMetric
case class ResumePartitionsFailed(error: IllegalStateException) extends ConsumerMetric
case class SeekToOffsetFailed(error: IllegalStateException) extends ConsumerMetric
