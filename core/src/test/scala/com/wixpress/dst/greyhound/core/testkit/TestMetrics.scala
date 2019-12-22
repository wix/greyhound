package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import zio._

trait TestMetrics extends Metrics[GreyhoundMetric] {
  val metrics: TestMetrics.Service
}

object TestMetrics {
  trait Service extends Metrics.Service[GreyhoundMetric] {
    def queue: Queue[GreyhoundMetric]
  }

  def make: Managed[Nothing, TestMetrics] =
    Queue.unbounded[GreyhoundMetric].toManaged_.map { q =>
      new TestMetrics {
        override val metrics: Service = new Service {
          override def report(metric: GreyhoundMetric): UIO[_] = q.offer(metric)
          override def queue: Queue[GreyhoundMetric] = q
        }
      }
    }
}
