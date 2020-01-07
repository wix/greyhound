package com.wixpress.dst.greyhound.core.testkit

import zio.{Promise, Ref, UIO, ZIO}

trait CountDownLatch {
  def countDown: UIO[Unit]
  def await: UIO[Unit]
}

object CountDownLatch {
  def make(count: Int): UIO[CountDownLatch] = for {
    ready <- Promise.make[Nothing, Unit]
    ref <- Ref.make(count)
  } yield new CountDownLatch {
    override def countDown: UIO[Unit] =
      ref.update(_ - 1).flatMap {
        case 0 => ready.succeed(()).unit
        case _ => ZIO.unit
      }

    override def await: UIO[Unit] = ready.await
  }
}
