package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.Schedule.{doUntil, spaced}
import zio.clock.Clock
import zio.duration._
import zio.{Has, RIO, ZIO}

package object testkit {
  type TestMetrics = Has[TestMetrics.Service] with GreyhoundMetrics

  def eventuallyZ[R <: Has[_], T](f: RIO[R, T], predicate: T => Boolean): ZIO[R, Throwable, (Partition, T)] =
    f.repeat(spaced(100.millis) && doUntil(predicate))
      .timeoutFail(new RuntimeException("eventuallyZ predicate failed"))(4.seconds)
      .provideSomeLayer[R](Clock.live)
}
