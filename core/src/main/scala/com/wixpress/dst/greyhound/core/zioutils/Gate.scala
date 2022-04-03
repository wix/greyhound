package com.wixpress.dst.greyhound.core.zioutils

import zio.UIO
import zio.stm.{STM, TRef}

trait Gate {
  def toggle(on: Boolean): UIO[Unit]

  def await(): UIO[Unit]
}

object Gate {
  def make(initiallyAllow: Boolean): UIO[Gate] = {
    TRef
      .makeCommit(initiallyAllow)
      .map(ref =>
        new Gate {
          override def toggle(on: Boolean): UIO[Unit] =
            ref.set(on).commit

          override def await(): UIO[Unit] =
            ref.get.flatMap(STM.check(_)).commit
        }
      )
  }
}
