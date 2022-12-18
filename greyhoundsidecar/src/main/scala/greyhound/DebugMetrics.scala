package greyhound

import com.wixpress.dst.greyhound
import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.metrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.{RLayer, Scope, Trace, UIO, ULayer, ZIO, ZLayer}


object DebugMetrics {
  val Value = report(metric => println(s"-Metrics- $metric"))

  val layer = ZLayer.succeed(Value)

  private def report(reportMetric: GreyhoundMetric => Unit): GreyhoundMetrics.Service  = {
    new GreyhoundMetrics.Service {
      override def report(metric: GreyhoundMetric)(implicit trace: Trace): UIO[Unit] = ZIO.succeed(reportMetric(metric))
    }
  }

}


