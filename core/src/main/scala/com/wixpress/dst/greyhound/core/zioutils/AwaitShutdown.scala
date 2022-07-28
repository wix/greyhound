package com.wixpress.dst.greyhound.core.zioutils

import zio._

trait AwaitShutdown { self =>
  def isShutDown (implicit trace: Trace): UIO[Boolean]
  def awaitShutdown (implicit trace: Trace): UIO[Nothing]
  def interruptOnShutdown[R, E, A](zio: ZIO[R, E, A]) (implicit trace: Trace): ZIO[R, E, A] =
    awaitShutdown raceFirst zio

  def or(other: AwaitShutdown) = new AwaitShutdown {
    override def isShutDown (implicit trace: Trace): UIO[Boolean] = self.isShutDown.zip(other.isShutDown).map(pair => pair._1 || pair._2)

    override def awaitShutdown (implicit trace: Trace): UIO[Nothing] = self.awaitShutdown raceFirst other.awaitShutdown
  }
}

object AwaitShutdown {

  val never = new AwaitShutdown {
    override def isShutDown (implicit trace: Trace): UIO[Boolean]    = ZIO.succeed(false)
    override def awaitShutdown (implicit trace: Trace): UIO[Nothing] = ZIO.never
  }

  trait OnShutdown                                                                 {
    def shuttingDown: UIO[Unit]
  }
  case class ShutdownPromise(onShutdown: OnShutdown, awaitShutdown: AwaitShutdown) {
    def toManaged (implicit trace: Trace) = ZIO.acquireRelease(ZIO.unit)(_ => onShutdown.shuttingDown)
  }

  def make (implicit trace: Trace)                                 = Promise.make[Nothing, Nothing].map { promise =>
    val onShutdown: OnShutdown = new OnShutdown {
      override def shuttingDown: UIO[Unit] = promise.die(new InterruptedException("shutting down")).unit
    }
    val awaitShutdown          = new AwaitShutdown {
      override def awaitShutdown (implicit trace: Trace): UIO[Nothing] = promise.await
      override def isShutDown (implicit trace: Trace): UIO[Boolean]    = promise.isDone
    }

    ShutdownPromise(onShutdown, awaitShutdown)
  }
  def makeManaged (implicit trace: Trace): URIO[Scope, AwaitShutdown] =
    ZIO.acquireRelease(make(trace))(_.onShutdown.shuttingDown)
      .map(_.awaitShutdown)
}
