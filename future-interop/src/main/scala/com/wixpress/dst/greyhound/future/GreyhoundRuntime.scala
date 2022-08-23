package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.{Console, RIO, Random, System, Unsafe, ZEnvironment}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

trait GreyhoundRuntime /*extends Runtime[GreyhoundRuntime.Env] */{
  protected val env: ZEnvironment[GreyhoundRuntime.Env]
  lazy val runtime = zio.Runtime.default.withEnvironment(env)

  def unsafeRun[A](zio: RIO[GreyhoundRuntime.Env, A]): A =
    Unsafe.unsafe{implicit us => runtime.unsafe.run(zio).getOrThrowFiberFailure()}


  def unsafeRunToFuture[A](zio: RIO[GreyhoundRuntime.Env, A]): Future[A] =
    Unsafe.unsafe{implicit us => runtime.unsafe.runToFuture(zio)}
}

object GreyhoundRuntime {
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  type Env = GreyhoundMetrics

  lazy val Live = GreyhoundRuntimeBuilder().build
}

case class GreyhoundRuntimeBuilder(metricsReporter: GreyhoundMetrics.Service = GreyhoundMetrics.Service.Live) {
  def withMetricsReporter(reporter: GreyhoundMetrics.Service): GreyhoundRuntimeBuilder =
    copy(metricsReporter = reporter)

  def build: GreyhoundRuntime =
    new GreyhoundRuntime {
      override val env = ZEnvironment(metricsReporter)
    }
}
