package com.wixpress.dst.greyhound.core.zioutils

import zio.{Promise, UIO, ZIO}

object PromiseSyntax {
  implicit class PromiseOps[E, A](val promise: Promise[E, A]) extends AnyVal {
    def map[B](f: A => B): ZIO[Any, Nothing, Promise[E, B]] = for {
      newPromise <- Promise.make[E, B]
      _ <- promise.await
        .flatMap(a => newPromise.complete(ZIO.succeed(f(a))))
        .catchAllCause(h => newPromise.complete(ZIO.halt(h)))
        .forkDaemon
    } yield newPromise

    def unit: UIO[Promise[E, Unit]] = map(_ => ())
  }
}
