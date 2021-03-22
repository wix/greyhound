package com.wixpress.dst.greyhound.core.metrics

import org.slf4j.LoggerFactory
import zio.blocking.Blocking
import zio.{Has, URIO, ZIO, ZLayer}

object GreyhoundMetrics {
  trait Service {
    self =>
    def report(metric: GreyhoundMetric): URIO[Blocking, Unit]

    def ++(service: Service): Service =
      (metric: GreyhoundMetric) =>
        self.report(metric) *> service.report(metric)
  }

  def report(metric: GreyhoundMetric): URIO[GreyhoundMetrics with Blocking, Unit] =
    ZIO.accessM(_.get.report(metric))

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
  lazy val live = Has(Service.Live)

  def fromReporter(report: GreyhoundMetric => Unit): GreyhoundMetrics.Service =
    metric => ZIO.effectTotal(report(metric))

}

trait GreyhoundMetric extends Product with Serializable

