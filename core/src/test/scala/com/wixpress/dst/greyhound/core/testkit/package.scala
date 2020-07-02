package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.clock.Clock
import zio.{Has, RIO, Schedule, UIO}
import zio.duration._

package object testkit {
  type TestMetrics = Has[TestMetrics.Service] with GreyhoundMetrics

  def eventuallyZ[R <: Has[_], T](f: RIO[R, T], predicate: T => Boolean) = {
    ((UIO(println("f")) *> f).repeat(Schedule.spaced(100.milliseconds) && Schedule.doUntil(predicate)).timeoutFail(new RuntimeException("predicate failed"))(4.seconds)).provideSomeLayer[R](Clock.live)
  }
}
