package com.wixpress.dst.greyhound.core.metrics

import com.wixpress.dst.greyhound.core.metrics
import org.slf4j.LoggerFactory
import zio.{CanFail, Cause, Clock, Duration, Exit, Trace, UIO, URIO, ZIO, ZLayer}

import scala.util.{Failure, Success, Try}

object GreyhoundMetrics {
  implicit val trace = Trace.empty
  trait Service {
    self =>
    protected def nanoTime = Clock.nanoTime

    def report(metric: GreyhoundMetric)(implicit trace: Trace): UIO[Unit]
    def reporting[R, E, A](zio: ZIO[R, E, A])(metric: MetricResult[E, A] => GreyhoundMetric)(implicit trace: Trace): ZIO[R, E, A] = {
      zio.exit
        .timedWith(nanoTime)
        .tap { case (duration, exit) => report(metric(MetricResult(exit, duration))) }
        .flatMap(_._2.foldZIO(e => ZIO.failCause(Cause.fail(e)), ZIO.succeed(_)))
    }

    def ++(service: Service): Service = new metrics.GreyhoundMetrics {
      override def report(metric: GreyhoundMetric)(implicit trace: Trace): UIO[Unit] =
        self.report(metric) *> service.report(metric)
    }
  }

  def report(metric: GreyhoundMetric): URIO[GreyhoundMetrics, Unit] =
    ZIO.environmentWithZIO(_.get.report(metric))

  def reporting[R, E, A](zio: ZIO[R, E, A])(
    metric: MetricResult[E, A] => GreyhoundMetric
  ): ZIO[GreyhoundMetrics with R, E, A] =
    ZIO.environmentWithZIO[GreyhoundMetrics](_.get.reporting(zio)(metric))

  object Service {
    lazy val Live = {
      val logger = LoggerFactory.getLogger("metrics")
      fromReporter(metric => logger.info(metric.toString))
    }
    val noop      = new Service {
      override def report(metric: GreyhoundMetric) (implicit trace: Trace): UIO[Unit] = ZIO.unit
    }
  }

  lazy val liveLayer = ZLayer.succeed(Service.Live)
  lazy val live      = Service.Live

  def fromReporter(report: GreyhoundMetric => Unit): GreyhoundMetrics.Service =
    new GreyhoundMetrics.Service {
      override def report(metric: GreyhoundMetric)(implicit trace: Trace): UIO[Unit] =
        ZIO.succeed(report(metric))
    }

  def noop: GreyhoundMetrics = Service.noop

  implicit class ZioOps[R, E: CanFail, A](val zio: ZIO[R, E, A]) {
    def reporting(metric: MetricResult[E, A] => GreyhoundMetric) = GreyhoundMetrics.reporting(zio)(metric)
  }

  implicit class UioOps[R, A](val zio: ZIO[R, Nothing, A]) {
    def reporting(metric: MetricResult[Nothing, A] => GreyhoundMetric) = GreyhoundMetrics.reporting(zio)(metric)
  }

  case class MetricResult[+E, +A](result: Exit[E, A], duration: Duration) {
    def map[B](f: A => B)                                 = copy(result = result.mapExit(f))
    def mapExit[B, E1 >: E](f: Exit[E, A] => Exit[E1, B]) = copy(result = f(result))
    def mapError[E1](f: E => E1)                          = copy(result = result.mapErrorExit(f))
    def toTry(implicit ev: E <:< Throwable): Try[A]       =
      result.foldExit(c => Failure(c.squashTrace), Success.apply)
    def value: Option[A]                                  = result.foldExit(_ => None, Some(_))
    def interrupted: Boolean                              = result.foldExit(_.isInterrupted, _ => false)
    def succeeded: Boolean                                = result.isSuccess
    def failed: Boolean                                   = !succeeded
  }
}

trait GreyhoundMetric extends Product with Serializable
