package com.wixpress.dst.greyhound.core.testkit

import org.specs2.execute.{AsResult, Error, Result}
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.core.{Fragment, Fragments}
import zio.{DefaultRuntime, FiberFailure, UManaged, ZIO}

trait BaseTest[R]
  extends SpecificationWithJUnit
    with DefaultRuntime {

  def env: UManaged[R]

  def run[R1 >: R, E, A](zio: ZIO[R1, E, A]): A =
    unsafeRun(env.use(zio.provide))

  def all[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*): ZIO[R1, E, Fragments] =
    ZIO.sequence(fragments).map(fragments => Fragments(fragments: _*))

  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] =
    new AsResult[ZIO[R1, E, A]] {
      override def asResult(t: => ZIO[R1, E, A]): Result =
        unsafeRunSync(env.use(t.provide)).fold(
          e => Error(FiberFailure(e)),
          a => ev.asResult(a))
    }

}
