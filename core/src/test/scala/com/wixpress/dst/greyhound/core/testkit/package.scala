package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.Schedule.{doUntil, spaced}
import zio.clock.Clock
import zio.duration._
import zio.{Has, RIO, Ref, UIO, ZIO}

package object testkit {
  type TestMetrics = Has[TestMetrics.Service] with GreyhoundMetrics

  def eventuallyZ[R <: Has[_], T](f: RIO[R, T])(predicate: T => Boolean): ZIO[R, Throwable, Unit] = for {
    resultRef <- Ref.make[Option[T]](None)
    timeoutRes <- f.flatMap(r =>
      resultRef.set(Some(r)) *> UIO(r))
      .repeat(spaced(100.millis) && doUntil(predicate))
      .timeout(4.seconds)
      .provideSomeLayer[R](Clock.live)
    result <- resultRef.get
    _ <- if (timeoutRes.isEmpty)
      ZIO.fail(new RuntimeException(s"eventuallyZ predicate failed. result: $result"))
    else
      ZIO.unit

  } yield ()
}
