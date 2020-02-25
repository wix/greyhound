package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.internal.{Platform, PlatformLive}
import zio.random.Random
import zio.system.System
import zio.{Runtime, ZEnv, ZIO}

trait GreyhoundRuntime extends Runtime[ZEnv with GreyhoundMetrics] {
  override val platform: Platform = PlatformLive.Default
}

object GreyhoundRuntime {
  type Env = ZEnv with GreyhoundMetrics

  val Live = GreyhoundRuntimeBuilder().build
}

case class GreyhoundRuntimeBuilder(metricsReporter: GreyhoundMetrics = GreyhoundMetric.Live) {
  def withMetricsReporter(reporter: GreyhoundMetrics): GreyhoundRuntimeBuilder =
    copy(metricsReporter = reporter)

  def withMetricsReporter(report: GreyhoundMetric => Unit): GreyhoundRuntimeBuilder =
    withMetricsReporter(new GreyhoundMetrics {
      override val metrics: Metrics.Service[GreyhoundMetric] =
        (metric: GreyhoundMetric) => ZIO.effectTotal(report(metric))
    })

  def build: GreyhoundRuntime =
    new GreyhoundRuntime {
      override val environment: Env =
        new Clock.Live
          with Console.Live
          with System.Live
          with Random.Live
          with Blocking.Live
          with GreyhoundMetrics {
          override val metrics: Metrics.Service[GreyhoundMetric] =
            metricsReporter.metrics
        }
    }
}
