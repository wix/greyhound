package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import org.slf4j.LoggerFactory
import zio._
import zio.blocking.Blocking

import scala.reflect.ClassTag

object TestMetrics {
  trait Service extends GreyhoundMetrics.Service {
    def queue: Queue[GreyhoundMetric]
    def reported: UIO[List[GreyhoundMetric]] = queue.takeAll
  }

  def make: UManaged[TestMetrics] = make()

  def make(andAlso: GreyhoundMetrics.Service = GreyhoundMetrics.Service.noop): UManaged[TestMetrics] = {
    val logger = LoggerFactory.getLogger("metrics")

    def reportLive(metric: GreyhoundMetric): URIO[Blocking, Unit] = {
      UIO(logger.info(metric.toString))
    }

    Queue.unbounded[GreyhoundMetric].toManaged_.map { q =>
      val service = new Service {
        override def report(metric: GreyhoundMetric): URIO[Blocking, Unit] =
          reportLive(metric) *> q.offer(metric).unit *> andAlso.report(metric)
        override def queue: Queue[GreyhoundMetric] = q
      }
      Has(service) ++ Has(service: GreyhoundMetrics.Service)
    }
  }

  def makeLayer: ULayer[TestMetrics] = makeLayer[Any]()
  def makeLayer[R](andAlso: R => GreyhoundMetrics.Service = (_:R) => GreyhoundMetrics.Service.noop): URLayer[R, TestMetrics] =
    ZLayer.fromFunctionManyManaged[R, Nothing, TestMetrics](a => make(andAlso(a)))

  def queue: URIO[TestMetrics, Queue[GreyhoundMetric]] =
    ZIO.access[TestMetrics](_.get.queue)

  def reported: URIO[TestMetrics, List[GreyhoundMetric]] =
    ZIO.accessM[TestMetrics](_.get.reported)

  def reportedOf[T <: GreyhoundMetric: ClassTag](filter: T => Boolean = (_:T) => true): URIO[TestMetrics, List[T]] =
    reported.map(ms => ms.collect {
      case m if implicitly[ClassTag[T]].runtimeClass.isAssignableFrom(m.getClass) => m.asInstanceOf[T]
    }.filter(filter))
}
