package greyhound

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.{URIO, ZIO, ZLayer}
import zio.blocking.Blocking

object DebugMetrics {
  val layer = ZLayer.succeed(report(metric => println(s"-Metrics- $metric")))

  private def report(report: GreyhoundMetric => Unit): GreyhoundMetrics.Service  =
    metric => ZIO.succeed(report(metric))

}


