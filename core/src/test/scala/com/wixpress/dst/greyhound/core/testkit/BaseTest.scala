package com.wixpress.dst.greyhound.core.testkit

import org.specs2.execute.{AsResult, Error, Result}
import org.specs2.mutable.SpecificationWithJUnit
import zio.{DefaultRuntime, FiberFailure, Managed, ZIO}

trait BaseTest[R]
  extends SpecificationWithJUnit
    with DefaultRuntime {

  def env: Managed[Nothing, R]

  def run[R1 >: R, E, A](zio: ZIO[R1, E, A])(implicit ev: AsResult[A]): Result =
    unsafeRunSync(env.use(zio.provide)).fold(
      e => Error(FiberFailure(e)),
      a => ev.asResult(a))

  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] =
    new AsResult[ZIO[R1, E, A]] {
      override def asResult(t: => ZIO[R1, E, A]): Result = run(t)
    }

}
