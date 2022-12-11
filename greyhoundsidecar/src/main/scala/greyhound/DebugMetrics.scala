package greyhound

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.{Trace, UIO, ZIO, ZLayer}


object DebugMetrics {
  val Value = report(metric => println(s"-Metrics- $metric"))

  val layer = ZLayer.succeed(report(metric => println(s"-Metrics- $metric")))

  private def report(reportMetric: GreyhoundMetric => Unit): GreyhoundMetrics.Service  = {
    new GreyhoundMetrics.Service {
      override def report(metric: GreyhoundMetric)(implicit trace: Trace): UIO[Unit] = ZIO.succeed(reportMetric(metric))
    }
  }

}


