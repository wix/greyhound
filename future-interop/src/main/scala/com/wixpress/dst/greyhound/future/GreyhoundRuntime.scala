package com.wixpress.dst.greyhound.future

import java.util.concurrent.Executors

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.visibility.zio.ZVisibility
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.internal.Platform
import zio.random.Random
import zio.system.System
import zio.{Has, Runtime}

import scala.concurrent.ExecutionContext

trait GreyhoundRuntime extends Runtime[GreyhoundRuntime.Env] {
  override val platform: Platform = Platform.default
}

object GreyhoundRuntime {
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  type ZEnv = Clock with Console with System with Random with Blocking

  val zenv = Has.allOf(
    Clock.Service.live,
    Console.Service.live,
    System.Service.live,
    Random.Service.live,
    Blocking.Service.live
  )

  type Env = ZEnv with GreyhoundMetrics

  val Live = GreyhoundRuntimeBuilder().build
}

case class GreyhoundRuntimeBuilder(metricsReporter: GreyhoundMetrics.Service = GreyhoundMetrics.Service.Live,
                                   userVisibility: ZVisibility = ZVisibility.Invisible) {
  def withMetricsReporter(reporter: GreyhoundMetrics.Service): GreyhoundRuntimeBuilder =
    copy(metricsReporter = reporter)

  def withUserVisibility(visiblity: ZVisibility): GreyhoundRuntimeBuilder =
    copy(userVisibility = visiblity)

  def build: GreyhoundRuntime =
    new GreyhoundRuntime {
      override val environment = GreyhoundRuntime.zenv ++ Has(metricsReporter)
    }
}
