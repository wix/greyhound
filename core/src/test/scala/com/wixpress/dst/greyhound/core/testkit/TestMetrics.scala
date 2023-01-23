package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import org.slf4j.LoggerFactory
import zio.managed.ZManaged
import zio.{Queue, Trace, UIO, URIO, ZEnvironment, ZIO, ZLayer}

import scala.reflect.ClassTag

object TestMetrics {
  trait Service extends GreyhoundMetrics.Service {
    def queue: Queue[GreyhoundMetric]
    def reported(implicit trace: Trace): UIO[List[GreyhoundMetric]] = queue.takeAll.map(_.toList)
  }

  def makeManagedEnv(implicit trace: Trace) =
    ZManaged.fromZIO(makeEnv())

  def buildEnv(implicit trace: Trace): ZEnvironment[TestMetrics] =
    zio.Unsafe.unsafe { implicit s => zio.Runtime.default.unsafe.run(makeEnv()).getOrThrowFiberFailure() }

  def makeEnv(implicit trace: Trace): UIO[ZEnvironment[TestMetrics]] = makeEnv()

  def makeEnv(andAlso: GreyhoundMetrics.Service = GreyhoundMetrics.Service.noop)(implicit trace: Trace): UIO[ZEnvironment[TestMetrics]] = {
    val logger = LoggerFactory.getLogger("metrics")

    def reportLive(metric: GreyhoundMetric): URIO[Any, Unit] = {
      ZIO.succeed(logger.info(metric.toString))
    }

    Queue.unbounded[GreyhoundMetric].map { q =>
      val service = new Service {
        override def report(metric: GreyhoundMetric)(implicit trace: Trace): URIO[Any, Unit] = {
          implicit val trace = Trace.empty
          (reportLive(metric) *> q.offer(metric).unit *> andAlso.report(metric)).unit
        }

        override def queue: Queue[GreyhoundMetric] = q
      }
      ZLayer.succeed(service) ++ ZLayer.succeed(service: GreyhoundMetrics.Service)
    }
  }.flatMap(_.build.provide(ZLayer.succeed(zio.Scope.global)))

  def queue(implicit trace: Trace): URIO[TestMetrics, Queue[GreyhoundMetric]] =
    ZIO.environment[TestMetrics].map(_.get[TestMetrics.Service].queue)

  def reported(implicit trace: Trace): URIO[TestMetrics, List[GreyhoundMetric]] =
    ZIO.environmentWithZIO[TestMetrics](_.get[TestMetrics.Service].reported)

  def reportedOf[T <: GreyhoundMetric: ClassTag](filter: T => Boolean = (_: T) => true)(implicit trace: Trace): URIO[TestMetrics, List[T]] =
    reported.map(ms =>
      ms.collect {
        case m if implicitly[ClassTag[T]].runtimeClass.isAssignableFrom(m.getClass) => m.asInstanceOf[T]
      }.filter(filter)
    )
}
