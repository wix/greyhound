package com.wixpress.dst.greyhound.core.testkit

import java.time.Instant

import zio.ZIO
import zio.duration.{Duration, _}
import zio.test.environment.TestClock

object TestClockUtils {
  def adjustClock(tickDuration: Duration) = {
    log(s"moved clock ${tickDuration.toMillis}") *> TestClock.adjust(tickDuration) *> waitALittle
  }

  private def waitALittle = {
    sleep(200.millis)
  }

  private def sleep(duration: Duration) = {
    zio.test.environment.live(zio.clock.sleep(duration))
  }

  private def log(str: String) = for {
    fiberId <- ZIO.fiberId
    _       <- ZIO.effectTotal(println(s"[${Instant.now}][$fiberId] $str"))
  } yield ()
}
