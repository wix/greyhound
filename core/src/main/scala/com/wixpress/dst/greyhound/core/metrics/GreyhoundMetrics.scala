package com.wixpress.dst.greyhound.core.metrics

import org.slf4j.LoggerFactory
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.Duration
import zio.{CanFail, Exit, Has, URIO, ZIO, ZLayer}

import scala.util.{Failure, Success, Try}

object GreyhoundMetrics {
  trait Service {
    self =>
    protected def nanoTime = Clock.Service.live.nanoTime

    def report(metric: GreyhoundMetric): URIO[Blocking, Unit]
    def reporting[R, E, A](zio: ZIO[R, E, A])(metric: MetricResult[E, A] => GreyhoundMetric): ZIO[Blocking with R, E, A] = {
      zio.run
        .timedWith(nanoTime)
        .tap { case (duration, exit) => report(metric(MetricResult(exit, duration))) }
        .flatMap(_._2.foldM(ZIO.halt(_), ZIO.succeed(_)))
    }

    def ++(service: Service): Service =
      (metric: GreyhoundMetric) => self.report(metric) *> service.report(metric)
  }

  def report(metric: GreyhoundMetric): URIO[GreyhoundMetrics with Blocking, Unit] =
    ZIO.accessM(_.get.report(metric))

  def reporting[R, E, A](zio: ZIO[R, E, A])(
    metric: MetricResult[E, A] => GreyhoundMetric
  ): ZIO[GreyhoundMetrics with Blocking with R, E, A] =
    ZIO.accessM(_.get.reporting(zio)(metric))

  object Service {
    lazy val Live = {
      val logger = LoggerFactory.getLogger("metrics")
      fromReporter(metric => logger.info(metric.toString))
    }
    val noop = new Service {
      override def report(metric: GreyhoundMetric): URIO[Blocking, Unit] = ZIO.unit
    }
  }

  lazy val liveLayer = ZLayer.succeed(Service.Live)
  lazy val live      = Has(Service.Live)

  def fromReporter(report: GreyhoundMetric => Unit): GreyhoundMetrics.Service =
    metric => ZIO.effectTotal(report(metric))

  def noop: GreyhoundMetrics = Has(Service.noop)

  implicit class ZioOps[R, E: CanFail, A](val zio: ZIO[R, E, A]) {
    def reporting(metric: MetricResult[E, A] => GreyhoundMetric) = GreyhoundMetrics.reporting(zio)(metric)
  }

  implicit class UioOps[R, A](val zio: ZIO[R, Nothing, A]) {
    def reporting(metric: MetricResult[Nothing, A] => GreyhoundMetric) = GreyhoundMetrics.reporting(zio)(metric)
  }

  case class MetricResult[+E, +A](result: Exit[E, A], duration: Duration) {
    def map[B](f: A => B)                                 = copy(result = result.map(f))
    def mapExit[B, E1 >: E](f: Exit[E, A] => Exit[E1, B]) = copy(result = f(result))
    def mapError[E1](f: E => E1)                          = copy(result = result.mapError(f))
    def toTry(implicit ev: E <:< Throwable): Try[A] =
      result.fold(c => Failure(c.squashTrace), Success.apply)
    def value: Option[A]     = result.fold(_ => None, Some(_))
    def interrupted: Boolean = result.fold(_.interrupted, _ => false)
    def succeeded: Boolean   = result.succeeded
    def failed: Boolean      = !succeeded
  }
}

trait GreyhoundMetric extends Product with Serializable
