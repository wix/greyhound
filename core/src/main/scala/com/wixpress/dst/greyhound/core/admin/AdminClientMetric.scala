package com.wixpress.dst.greyhound.core.admin

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.MetricResult

trait AdminClientMetric extends GreyhoundMetric

object AdminClientMetric {
  case class TopicConfigUpdated(topic: Topic,
                                configProperties: Map[String, ConfigPropOp],
                                incremental: Boolean,
                                attributes: Map[String, String],
                                result: MetricResult[Throwable, Unit]) extends AdminClientMetric

  case class TopicPartitionsIncreased(topic: Topic,
                                      newCount: Int,
                                      attributes: Map[String, String],
                                      result: MetricResult[Throwable, Unit])
    extends AdminClientMetric
}
