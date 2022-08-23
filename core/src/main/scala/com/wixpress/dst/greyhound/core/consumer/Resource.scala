package com.wixpress.dst.greyhound.core.consumer

import zio.{Trace, UIO, URIO, ZIO}

trait Resource[-R] { self =>
  def pause(implicit trace: Trace): URIO[R, Unit]

  def resume(implicit trace: Trace): URIO[R, Unit]

  def isAlive(implicit trace: Trace): URIO[R, Boolean]

  final def combine[R1 <: R](other: Resource[R1]): Resource[R1] =
    new Resource[R1] {
      override def pause(implicit trace: Trace): URIO[R1, Unit] =
        self.pause *> other.pause

      override def resume(implicit trace: Trace): URIO[R1, Unit] =
        self.resume *> other.resume

      override def isAlive(implicit trace: Trace): URIO[R1, Boolean] = (self.isAlive zipWith other.isAlive)(_ && _)
    }
}

object Resource {
  val Empty = new Resource[Any] {
    override def pause(implicit trace: Trace): UIO[Unit]      = ZIO.unit
    override def resume(implicit trace: Trace): UIO[Unit]     = ZIO.unit
    override def isAlive(implicit trace: Trace): UIO[Boolean] = ZIO.succeed(true)
  }
}
