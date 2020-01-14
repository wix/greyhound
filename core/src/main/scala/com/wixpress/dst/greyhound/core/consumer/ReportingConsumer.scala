package com.wixpress.dst.greyhound.core.consumer
import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.{Offset, Topic}
import zio.duration.Duration
import zio.{RIO, ZIO}

case class ReportingConsumer[R](internal: Consumer[R])
  extends Consumer[R with GreyhoundMetrics] {

  override def poll(timeout: Duration): RIO[R with GreyhoundMetrics, Records] =
    internal.poll(timeout)

  override def subscribe(topics: Set[Topic]): RIO[R with GreyhoundMetrics, Unit] =
    ZIO.when(topics.nonEmpty) {
      Metrics.report(Subscribing(topics)) *>
        internal.subscribe(topics)
    }

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

}

sealed trait ConsumerMetric extends GreyhoundMetric
case class Subscribing(topics: Set[Topic]) extends ConsumerMetric
case class CommittingOffsets(offsets: Map[TopicPartition, Offset]) extends ConsumerMetric
case class Pausing(partitions: Set[TopicPartition]) extends ConsumerMetric
case class Resuming(partitions: Set[TopicPartition]) extends ConsumerMetric
case class Seeking(partition: TopicPartition, offset: Offset) extends ConsumerMetric
