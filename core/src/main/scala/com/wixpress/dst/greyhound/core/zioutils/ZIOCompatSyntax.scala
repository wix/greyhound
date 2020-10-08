package com.wixpress.dst.greyhound.core.zioutils

import zio.Schedule.identity
import zio.ZManaged.ReleaseMap
import zio.clock.Clock
import zio.{ExecutionStrategy, Exit, Schedule, UIO, URIO, ZIO, ZManaged}

/**
  * for compatibility of code written against 1.0 to zio-RC21
  */
object ZIOCompatSyntax {
  implicit class ZIOOps[R, E, A](val self: ZIO[R, E, A]) extends AnyVal {

    /**
      * Repeats this effect while its error satisfies the specified predicate.
      */
    final def repeatWhile(f: A => Boolean): ZIO[R with Clock, E, A] =
      self.repeat(Schedule.recurWhile(f))

    /**
      * Repeats this effect until its result satisfies the specified effectful predicate.
      */
    final def repeatUntilM(f: A => UIO[Boolean]): ZIO[R with Clock, E, A] =
      self.repeat(Schedule.recurUntilM(f))

    /**
      * Repeats this effect while its result satisfies the specified effectful predicate.
      */
    final def repeatWhileM(f: A => UIO[Boolean]): ZIO[R with Clock, E, A] =
      self.repeat(Schedule.recurWhileM(f))
  }

  implicit class ScheduleOps(val self: zio.Schedule.type) {
    def recurUntil[A](f: A => Boolean): Schedule[Any, A, A] =
      identity[A].untilInput(f)

    def recurUntilM[A, Env](f: A => UIO[Boolean]): Schedule[Env, A, A] =
      identity[A].untilInputM(f)

    /**
      * A schedule that recurs for as long as the predicate evaluates to true.
      */
    def recurWhile[A](f: A => Boolean): Schedule[Any, A, A] =
      recurWhileM(a => ZIO.succeed(f(a)))

    /**
      * A schedule that recurs for as long as the effectful predicate evaluates to true.
      */
    def recurWhileM[A](f: A => UIO[Boolean]): Schedule[Any, A, A] =
      identity[A].whileInputM(f)
  }

  implicit class ZManagedOps[R, E, A](val zm: ZManaged[R, E, A]) extends AnyVal {
    def reserve: UIO[CompatReservation[R, E, A]] =
      ReleaseMap.make.map { releaseMap =>
        CompatReservation(
          zm.zio.provideSome[R]((_, releaseMap)).map(_._2),
          releaseMap.releaseAll(_, ExecutionStrategy.Sequential)
        )
      }
  }

  case class CompatReservation[-R, +E, +A](acquire: ZIO[R, E, A], release: Exit[Any, Any] => URIO[R, Any])
}
