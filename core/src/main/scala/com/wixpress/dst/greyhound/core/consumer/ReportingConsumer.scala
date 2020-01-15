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
      _ <- Metrics.report(Subscribing(topics))
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
    internal.poll(timeout)

  override def commit(offsets: Map[TopicPartition, Offset]): RIO[R with GreyhoundMetrics, Unit] =
    ZIO.when(offsets.nonEmpty) {
      Metrics.report(CommittingOffsets(offsets)) *>
        internal.commit(offsets)
    }

  override def pause(partitions: Set[TopicPartition]): RIO[R with GreyhoundMetrics, Unit] =
    ZIO.when(partitions.nonEmpty) {
      Metrics.report(Pausing(partitions)) *>
        internal.pause(partitions)
    }

  override def resume(partitions: Set[TopicPartition]): RIO[R with GreyhoundMetrics, Unit] =
    ZIO.when(partitions.nonEmpty) {
      Metrics.report(Resuming(partitions)) *>
        internal.resume(partitions)
    }

  override def seek(partition: TopicPartition, offset: Offset): RIO[R with GreyhoundMetrics, Unit] =
    Metrics.report(Seeking(partition, offset)) *> internal.seek(partition, offset)

  override def partitionsFor(topic: Topic): RIO[R with GreyhoundMetrics, Set[TopicPartition]] =
    internal.partitionsFor(topic)

}

sealed trait ConsumerMetric extends GreyhoundMetric
case class Subscribing(topics: Set[Topic]) extends ConsumerMetric
case class CommittingOffsets(offsets: Map[TopicPartition, Offset]) extends ConsumerMetric
case class Pausing(partitions: Set[TopicPartition]) extends ConsumerMetric
case class Resuming(partitions: Set[TopicPartition]) extends ConsumerMetric
case class Seeking(partition: TopicPartition, offset: Offset) extends ConsumerMetric
case class PartitionsAssigned(partitions: Set[TopicPartition]) extends ConsumerMetric
case class PartitionsRevoked(partitions: Set[TopicPartition]) extends ConsumerMetric
