package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import org.slf4j.LoggerFactory
import zio._

object TestMetrics {
  trait Service extends GreyhoundMetrics.Service {
    def queue: Queue[GreyhoundMetric]
    def reported: UIO[List[GreyhoundMetric]] = queue.takeAll
  }

  def make: UManaged[TestMetrics] = {
    val logger = LoggerFactory.getLogger("metrics")

    def reportLive(metric: GreyhoundMetric) = {
      UIO(logger.info(metric.toString))
    }

    Queue.unbounded[GreyhoundMetric].toManaged_.map { q =>
      val service = new Service {
        override def report(metric: GreyhoundMetric): UIO[Unit] = reportLive(metric) *> q.offer(metric).unit
        override def queue: Queue[GreyhoundMetric] = q
      }
      Has(service) ++ Has(service: GreyhoundMetrics.Service)
    }
  }

  def queue: URIO[TestMetrics, Queue[GreyhoundMetric]] =
    ZIO.access[TestMetrics](_.get.queue)

  def reported: URIO[TestMetrics, List[GreyhoundMetric]] =
    ZIO.accessM[TestMetrics](_.get.reported)
}
