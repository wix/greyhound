package com.wixpress.dst.greyhound.core.consumer.retry

import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.core.Fragment
import zio.duration._

import scala.math.{abs, pow}

class ZRetryConfigTest extends SpecificationWithJUnit {

  "ZRetryConfigTest" should {
    Fragment.foreach(Seq(
      Params(max = -1, init = 10, mult = 30),
      Params(max = 0, init = 10, mult = 30),
      Params(max = 5, init = -10, mult = 2),
      Params(max = 5, init = 10, mult = 3),
      Params(max = 5, init = 10, mult = 1),
      Params(max = 5, init = 10, mult = -1)
    )) { params =>
      s"exponentialBackoff with maxMultiplications for params: ${params}" in {
        val max = params.max
        val init = params.init
        val mult = params.mult

        val config = ZRetryConfig.exponentialBackoffBlockingRetry(init.millis, max, mult)
        val backoffs = config.blockingBackoffs()

        val absMult = abs(params.mult)
        val safeInit = if(init < 10) 10 else init
        for (i <- 0 to max) yield {backoffs(i)} mustEqual (pow((1 + absMult), i) * safeInit).toLong.millis
        val maxMult = math.max(0, max)
        val lastDurationToCheck = abs(max + 1) * 2
        val firstDurationToCheck = math.max(0, max + 1)
        for (i <- firstDurationToCheck to lastDurationToCheck) yield backoffs(i) mustEqual (pow((1 + absMult), maxMult) * safeInit).toLong.millis
      }
    }


    Fragment.foreach(Seq(
      Params2(maxDuration = 160, maxMult = 4, init = 10, mult = 1),
      Params2(maxDuration = 0, maxMult = 0, init = 10, mult = 1),
      Params2(maxDuration = -20, maxMult = 0, init = 10, mult = 1),
      Params2(maxDuration = -20, maxMult = 0, init = -10, mult = 1)
    )) { params =>
      s"exponentialBackoff with maxMultiplications for params: ${params}" in {
        val maxDuration = params.maxDuration
        val init = params.init
        val mult = params.mult
        val maxMult = params.maxMult

        val config = ZRetryConfig.exponentialBackoffBlockingRetry(init.millis, maxDuration.millis, mult)
        val backoffs = config.blockingBackoffs()

        val absMult = abs(params.mult)
        val safeInit = if(init < 10) 10 else init
        for (i <- 0 to maxMult) yield {backoffs(i)} mustEqual (pow((1 + absMult), i) * safeInit).toLong.millis
        val lastDurationToCheck = abs(maxMult + 1) * 2
        val firstDurationToCheck = math.max(0, maxMult + 1)
        for (i <- firstDurationToCheck to lastDurationToCheck) yield backoffs(i) mustEqual (pow((1 + absMult), maxMult) * safeInit).toLong.millis
      }
    }
  }
}

case class Params(max: Int, init: Int, mult: Int)
case class Params2(maxDuration: Int, maxMult: Int, init: Int, mult: Int)
