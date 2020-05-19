package com.wixpress.dst.greyhound.core.metrics

import org.slf4j.LoggerFactory
import zio.ZIO
import zio.macros.annotation.delegate
import zio.macros.delegate.Mix

trait GreyhoundMetric extends Product with Serializable

object GreyhoundMetric {
  type GreyhoundMetrics = Metrics[GreyhoundMetric]

  trait Live extends GreyhoundMetrics {
    private val logger = LoggerFactory.getLogger("metrics")

    override val metrics: Metrics.Service[GreyhoundMetric] =
      (metric: GreyhoundMetric) => ZIO.effectTotal(logger.info(metric.toString))
  }

  object Live extends Live

  implicit class Ops(val self: GreyhoundMetrics) {
    def combine[R](r: R)(implicit ev: Mix[R, GreyhoundMetrics]): R with GreyhoundMetrics = {
      class WithGreyhoundMetrics(@delegate r: R) extends GreyhoundMetrics {
        override val metrics = self.metrics
      }
      ev.mix(r, new WithGreyhoundMetrics(r))
    }
  }
}
