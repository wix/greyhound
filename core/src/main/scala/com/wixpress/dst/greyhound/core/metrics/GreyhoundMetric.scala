package com.wixpress.dst.greyhound.core.metrics

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
