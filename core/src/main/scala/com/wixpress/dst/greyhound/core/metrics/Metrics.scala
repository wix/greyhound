package com.wixpress.dst.greyhound.core.metrics

import org.slf4j.LoggerFactory
import zio.{UIO, URIO, ZIO}

trait Metrics[-A] {
  val metrics: Metrics.Service[A]
}

object Metrics {
  trait Service[-A] {
    def report(metric: A): UIO[_]
  }

  def report[A](metric: A): URIO[Metrics[A], _] =
    ZIO.accessM[Metrics[A]](_.metrics.report(metric))

  case class Live[A]() extends Service[A] {
    private val logger = LoggerFactory.getLogger("metrics")

    override def report(metric: A): UIO[_] =
      ZIO.effectTotal(logger.info(metric.toString))
  }
}
