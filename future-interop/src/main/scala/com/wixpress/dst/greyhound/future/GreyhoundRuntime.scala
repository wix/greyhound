package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.internal.{Platform, PlatformLive}
import zio.random.Random
import zio.system.System
import zio.{Runtime, ZEnv}

trait GreyhoundRuntime extends Runtime[ZEnv with GreyhoundMetrics] {
  override val platform: Platform = PlatformLive.Default
}

object GreyhoundRuntime {
  type Env = ZEnv with GreyhoundMetrics

  val Live = withMetrics(GreyhoundMetric.Live)

  def withMetrics(metrics: GreyhoundMetrics): GreyhoundRuntime =
    new GreyhoundRuntime {
      private val metricsService = metrics.metrics

      override val environment: Env =
        new Clock.Live
          with Console.Live
          with System.Live
          with Random.Live
          with Blocking.Live
          with GreyhoundMetrics {
          override val metrics: Metrics.Service[GreyhoundMetric] =
            metricsService
        }
    }
}
