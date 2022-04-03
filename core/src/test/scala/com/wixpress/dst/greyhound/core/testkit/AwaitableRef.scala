package com.wixpress.dst.greyhound.core.testkit

import zio._
import zio.duration._
import zio.stm.{STM, TRef}

import scala.concurrent.TimeoutException

trait AwaitableRef[A] {
  def set(a: A): UIO[Unit]
  def update(f: A => A): UIO[Unit]
  def updateAndGet(f: A => A): UIO[A]
  def get: UIO[A]
  def await(p: A => Boolean, timeout: Duration = 1000.millis): IO[TimeoutException, A]
  def awaitChangeAfter[R, E, A1](effect: => ZIO[R, E, Any], timeout: Duration = 1000.millis): ZIO[R, E, A] =
    for {
      before <- get
      _      <- effect
      res    <- await(_ != before, timeout).orDie
    } yield res
}

object AwaitableRef {

  private val liveClock = Has(clock.Clock.Service.live)

  def make[A](a: A): UIO[AwaitableRef[A]] =
    for {
      runtime <- ZIO.runtime[Any]
      ref     <- TRef.make(a).commit
    } yield new AwaitableRef[A] {

      override def set(a: A): UIO[Unit] = update(_ => a).unit

      override def update(f: A => A): UIO[Unit] = {
        ref.update(f).commit
      }

      override def updateAndGet(f: A => A): UIO[A] =
        ref.updateAndGet(f).commit

      override def get: UIO[A] = ref.get.commit

      override def await(p: A => Boolean, timeout: Duration = 1000.millis): IO[TimeoutException, A] =
        (
          for {
            v <- ref.get
            _ <- STM.check(p(v))
          } yield v
        ).commit
          .timeoutFail(
            new TimeoutException(
              s"AwaitableRef: timed out waiting for condition [timeout: $timeout]\ncurrent value: ${runtime.unsafeRunTask(ref.get.commit)}"
            )
          )(timeout)
          .provide(liveClock)
    }
}
