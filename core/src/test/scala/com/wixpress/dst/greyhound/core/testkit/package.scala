package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.Schedule._
import zio.Schedule
import com.wixpress.dst.greyhound.core.zioutils.ZIOCompatSyntax._
import zio.clock.Clock
import zio.duration._
import zio.{Has, RIO, Ref, UIO, ZIO}

package object testkit {
  type TestMetrics = Has[TestMetrics.Service] with GreyhoundMetrics

  def eventuallyZ[R <: Has[_], T](f: RIO[R, T])(predicate: T => Boolean): ZIO[R, Throwable, Unit] =
    eventuallyTimeout(f)(predicate)(4.seconds)

  def eventuallyTimeout[R <: Has[_], T](f: RIO[R, T])(predicate: T => Boolean)(timeout: Duration): ZIO[R, Throwable, Unit] =
    for {
      resultRef <- Ref.make[Option[T]](None)
      timeoutRes <- f.flatMap(r =>
        resultRef.set(Some(r)) *> UIO(r))
        .repeat(spaced(100.millis) && Schedule.recurUntil(predicate))
        .timeout(timeout)
        .provideSomeLayer[R](Clock.live)
      result <- resultRef.get
      _ <- ZIO.when(timeoutRes.isEmpty)(ZIO.fail(new RuntimeException(s"eventuallyZ predicate failed after ${timeout.toMillis} milliseconds. result: $result")))
    } yield ()

}
