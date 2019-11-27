package com.wixpress.dst.greyhound.core.metrics

import zio.UIO

trait Metrics {
  def metrics: Metrics.Service
}

object Metrics {
  trait Service {
    def report(metric: Metric): UIO[_]
  }
}

sealed trait Metric
