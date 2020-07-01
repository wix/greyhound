package com.wixpress.dst.greyhound.core.zioutils

import zio.ZManaged.ReleaseMap
import zio._

object ZManagedSyntax {
  /**
    * For compatibility with ZManaged in RC17
    * based on https://discordapp.com/channels/629491597070827530/630498701860929559/711880863683575881
    */
  implicit class ZManagedOps[R, E, A](val zm: ZManaged[R, E, A]) extends AnyVal {
    def reserve: ZIO[R, E, Reservation[A]] =
      ReleaseMap.make.flatMap { releaseMap =>
        zm.zio.provideSome[R]((_, releaseMap)).map { case (_, a) =>
          Reservation(a, exit => releaseMap.releaseAll(exit, ExecutionStrategy.Sequential))
        }
      }
  }
  case class Reservation[A](acquired: A, release: Exit[Any, Any] => UIO[Any]) {
    def acquire = UIO(acquired)
  }
}
