package greyhound

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.{URIO, ZIO, ZLayer}
import zio.blocking.Blocking

object DebugMetrics {
  val Value = report(metric => println(s"-Metrics- $metric"))

  val layer = ZLayer.succeed(report(metric => println(s"-Metrics- $metric")))

  private def report(report: GreyhoundMetric => Unit): GreyhoundMetrics.Service  =
    metric => ZIO.succeed(report(metric))

}


