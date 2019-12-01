package com.wixpress.dst.greyhound.core.metrics

import com.wixpress.dst.greyhound.core.{Record, TopicName}

sealed trait GreyhoundMetric extends Product with Serializable

object GreyhoundMetric {
  type GreyhoundMetrics = Metrics[GreyhoundMetric]
}

case class Subscribing(topics: Set[TopicName]) extends GreyhoundMetric
case class SubmittingRecord[K, V](record: Record[K, V]) extends GreyhoundMetric
case class StartingRecordsProcessor(processor: Int) extends GreyhoundMetric
case class StoppingRecordsProcessor(processor: Int) extends GreyhoundMetric
case class HandlingRecord[K, V](record: Record[K, V], processor: Int) extends GreyhoundMetric
