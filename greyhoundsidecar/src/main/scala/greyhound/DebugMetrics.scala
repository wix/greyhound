package greyhound

import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.PolledRecords
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.{URIO, ZIO, ZLayer}


object DebugMetrics {
  val Value = report(metric => println(s"-Metrics- $metric"))

  val layer = ZLayer.succeed(report(metric => println(s"-Metrics- $metric")))

  private def report(report: GreyhoundMetric => Unit): GreyhoundMetrics.Service  = {
    case _: PolledRecords => ZIO.unit
    case metric => ZIO.succeed(report(metric))
  }

}


