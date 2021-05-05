package com.wixpress.dst.greyhound.core.testkit

import org.specs2.execute.AsResult
import zio.test.TestResult
import zio.{NeedsEnv, RIO, ZIO}

object TestCtx {
  def apply[R, CTX](make: => RIO[R, CTX]) = new Wrapper[R, CTX](make)

  def forSpecs2[R, CTX](make: => RIO[R, CTX]) = new SpecsWrapper[R, CTX](make)

  class Wrapper[-R, +CTX](body: => RIO[R, CTX]) {
    def make = body
  }

  object Wrapper {
    implicit class HasEnvOps[R, CTX](w: Wrapper[R, CTX]) {
      def apply[R1, E](f: CTX => ZIO[R with R1, E, TestResult])(implicit ev: NeedsEnv[R]) = w.make.flatMap(f)
    }
    implicit class NoEnvOps[CTX](w: Wrapper[Any, CTX]) {
      def apply[R1, E](f: CTX => ZIO[R1, E, TestResult]) = w.make.flatMap(f)
    }
  }

  class SpecsWrapper[R, CTX](make: => RIO[R, CTX]) {
    def apply[R1, E, A: AsResult](f: CTX => ZIO[R with R1, E, A]) = make.flatMap(f)
  }
}
