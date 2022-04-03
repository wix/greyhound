package com.wixpress.dst.greyhound.core.consumer.retry

import java.lang.Math.{pow => power}
import java.util.concurrent.TimeUnit

import com.wixpress.dst.greyhound.core.Offset
import zio.duration.{Duration => ZDuration}

import scala.math.{abs, log10, max}
import scala.util.Try
import zio.duration._

object ExponentialBackoffCalculator {
  def exponentialBackoffs(
    initialInterval: ZDuration,
    maximalInterval: ZDuration,
    backOffMultiplier: Float,
    infiniteRetryMaxInteval: Boolean
  ): Seq[ZDuration] = {
    val logOfMultipier = (x: Double, multiplier: Float) => log10(x) / log10(multiplier)

    def calcMaxMultiplications(initialInterval: ZDuration, maximalInterval: ZDuration, backOffMultiplier: Float) = {
      val safeInitialInterval = math.max(initialInterval.toMillis, 10)
      val safeMaximalInterval = math.max(safeInitialInterval, maximalInterval.toMillis)
      val relativeInterval: Offset = Try {
        safeMaximalInterval / initialInterval.toMillis
      }.getOrElse(1)

      math.ceil(logOfMultipier(relativeInterval, backOffMultiplier + 1)).toInt
    }

    val maxMultiplications = calcMaxMultiplications(initialInterval, maximalInterval, backOffMultiplier)
    exponentialBackoffs(initialInterval, maxMultiplications, backOffMultiplier, infiniteRetryMaxInteval)
  }

  def exponentialBackoffs(
    initialInterval: ZDuration,
    maxMultiplications: Int,
    backOffMultiplier: Float,
    infiniteRetryMaxInterval: Boolean
  ): Seq[ZDuration] = {
    val absBackOffMultiplier   = abs(backOffMultiplier)
    val safeMaxMultiplications = max(0, maxMultiplications)

    val safeInitialInterval = if (initialInterval.toMillis < 10) ZDuration(10, TimeUnit.MILLISECONDS) else initialInterval

    val maxDuration = safeInitialInterval.toMillis * power((1 + absBackOffMultiplier), safeMaxMultiplications).toLong

    val infiniteDurations = Stream.iterate(safeInitialInterval)(prevInterval => {
      val calclatedDuration = prevInterval * (1 + absBackOffMultiplier)

      if (calclatedDuration.toMillis > maxDuration)
        ZDuration(maxDuration, TimeUnit.MILLISECONDS)
      else
        calclatedDuration
    })
    if (!infiniteRetryMaxInterval)
      infiniteDurations.take(maxMultiplications)
    else
      infiniteDurations
  }
}
