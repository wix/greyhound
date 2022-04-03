package com.wixpress.dst.greyhound.core.consumer

import zio.{UIO, URIO, ZIO}

trait Resource[-R] { self =>
  def pause: URIO[R, Unit]

  def resume: URIO[R, Unit]

  def isAlive: URIO[R, Boolean]

  final def combine[R1 <: R](other: Resource[R1]): Resource[R1] =
    new Resource[R1] {
      override def pause: URIO[R1, Unit] =
        self.pause *> other.pause

      override def resume: URIO[R1, Unit] =
        self.resume *> other.resume

      override def isAlive: URIO[R1, Boolean] = (self.isAlive zipWith other.isAlive)(_ && _)
    }
}

object Resource {
  val Empty = new Resource[Any] {
    override val pause: UIO[Unit]      = ZIO.unit
    override val resume: UIO[Unit]     = ZIO.unit
    override val isAlive: UIO[Boolean] = ZIO.succeed(true)
  }
}
