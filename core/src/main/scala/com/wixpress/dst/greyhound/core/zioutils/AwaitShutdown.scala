package com.wixpress.dst.greyhound.core.zioutils

import zio._

trait AwaitShutdown { self =>
  def isShutDown: UIO[Boolean]
  def awaitShutdown: UIO[Nothing]
  def interruptOnShutdown[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A] =
    awaitShutdown raceFirst zio

  def tapShutdown(f: Throwable => UIO[Unit]) = new AwaitShutdown {
    override def isShutDown: UIO[Boolean] = self.isShutDown
    override def awaitShutdown: UIO[Nothing] = self.awaitShutdown.tapCause(c => f(c.squashTrace))
  }
}

object AwaitShutdown {

  val never = new AwaitShutdown {
    override def isShutDown: UIO[Boolean] = UIO(false)
    override def awaitShutdown: UIO[Nothing] = ZIO.never
  }

  trait OnShutdown {
    def shuttingDown: UIO[Unit]
  }
  case class ShutdownPromise(onShutdown: OnShutdown, awaitShutdown: AwaitShutdown) {
    def toManaged = ZIO.unit.toManaged(_ => onShutdown.shuttingDown)
  }

  def make = Promise.make[Nothing, Nothing].map { promise =>
    val onShutdown: OnShutdown = new OnShutdown {
      override def shuttingDown: UIO[Unit] = promise.die(new InterruptedException("shutting down")).unit
    }
    val awaitShutdown = new AwaitShutdown {
      override def awaitShutdown: UIO[Nothing] = promise.await
      override def isShutDown: UIO[Boolean] = promise.isDone
    }

    ShutdownPromise(onShutdown, awaitShutdown)
  }
  def makeManaged: UManaged[AwaitShutdown] = make.toManaged(_.onShutdown.shuttingDown).map(_.awaitShutdown)
}
