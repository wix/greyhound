package com.wixpress.dst.greyhound.core.metrics

import com.wixpress.dst.greyhound.core.{Offset, Record, TopicName}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import zio.ZIO

trait GreyhoundMetric extends Product with Serializable

object GreyhoundMetric {
  type GreyhoundMetrics = Metrics[GreyhoundMetric]

  trait Live extends GreyhoundMetrics {
    private val logger = LoggerFactory.getLogger("metrics")

    override val metrics: Metrics.Service[GreyhoundMetric] =
      (metric: GreyhoundMetric) => ZIO.effectTotal(logger.info(metric.toString))
  }
}

case class Subscribing(topics: Set[TopicName]) extends GreyhoundMetric
case class SubmittingRecord[K, V](record: Record[K, V]) extends GreyhoundMetric
case class StartingRecordsProcessor(processor: Int) extends GreyhoundMetric
case class StoppingRecordsProcessor(processor: Int) extends GreyhoundMetric
case class HandlingRecord[K, V](record: Record[K, V], processor: Int) extends GreyhoundMetric
case class CommittingOffsets(offsets: Map[TopicPartition, Offset]) extends GreyhoundMetric
