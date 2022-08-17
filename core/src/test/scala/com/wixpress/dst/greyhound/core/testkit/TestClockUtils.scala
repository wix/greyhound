package com.wixpress.dst.greyhound.core.testkit

import java.time.Instant
import zio.ZIO
import zio.test.TestClock
import zio.{Duration, _}

object TestClockUtils {
  def adjustClock(tickDuration: Duration)(implicit trace: Trace) = {
    log(s"moved clock ${tickDuration.toMillis}") *> TestClock.adjust(tickDuration) *> waitALittle
  }

  private def waitALittle (implicit trace: Trace) = {
    sleep(200.millis)
  }

  private def sleep(duration: Duration)(implicit trace: Trace) = {
    zio.Clock.sleep(duration)
  }

  private def log(str: String)(implicit trace: Trace) = for {
    fiberId <- ZIO.fiberId
    _       <- ZIO.succeed(println(s"[${Instant.now}][$fiberId] $str"))
  } yield ()
}
