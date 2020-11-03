package com.wixpress.dst.greyhound.core.testkit

import zio.{Promise, Ref, UIO, ZIO}

trait CountDownLatch {
  def countDown: UIO[Unit] = countDown(1)
  def countDown(n: Int): UIO[Unit]
  def await: UIO[Unit]
  def count: UIO[Int]
}

object CountDownLatch {
  def make(count: Int): UIO[CountDownLatch] = for {
    ready <- Promise.make[Nothing, Unit]
    ref <- Ref.make(count)
  } yield new CountDownLatch {
    override def countDown(n: Int): UIO[Unit] =
      ref.updateAndGet{ v =>
        val res = v - n
        if(res < 0) 0
        else res
      }.flatMap {
        case 0 => ready.succeed(()).unit
        case _ => ZIO.unit
      }

    override def await: UIO[Unit] = ready.await

    override def count: UIO[Int] = ref.get
  }
}
