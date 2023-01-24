package com.wixpress.dst.greyhound.core.zioutils

import zio.managed._
import zio.{durationInt, Duration, Exit, Trace, UIO, ZIO}

import scala.concurrent.TimeoutException
import zio.managed._

case class AcquiredManagedResource[T](resource: T, onRelease: UIO[Unit], runtime: zio.Runtime[Any]) {
  def release()(implicit trace: Trace): Unit =
    zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(onRelease).getOrThrowFiberFailure() }
}

object AcquiredManagedResource {
  def acquire[R <: Any: zio.Tag, T](
    resources: ZManaged[R, Throwable, T],
    releaseTimeout: Duration = 10.seconds
  )(implicit trace: Trace): ZIO[R, Throwable, AcquiredManagedResource[T]] = for {
    runtime  <- ZIO.runtime[Any]
    r        <- resources.reserve
    env      <- ZIO.environment[R]
    acquired <- r.acquire
  } yield {
    val releaseWithTimeout = r
      .release(Exit.unit)
      .disconnect
      .timeoutFail(new TimeoutException("release timed out"))(releaseTimeout)
      .provideEnvironment(env)
      .orDie
      .unit
    AcquiredManagedResource(acquired, releaseWithTimeout, runtime)
  }
}
