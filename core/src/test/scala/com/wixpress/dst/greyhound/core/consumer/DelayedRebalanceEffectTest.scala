package com.wixpress.dst.greyhound.core.consumer

import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

class DelayedRebalanceEffectTest extends SpecificationWithJUnit {
  "sequence with  *> operator" in new ctx {
    val combined = DelayedRebalanceEffect(log("1")) *> DelayedRebalanceEffect(log("2"))
    combined.run()
    logged ===  Seq("1", "2")
  }

  "tapError" in new ctx {
    val e = new Throwable("foo")
    val tle = DelayedRebalanceEffect(throw e).tapError(e => log(e.getMessage))
    tle.run() must throwA(e)
    logged === Seq("foo")
  }

  "catchAll" in new ctx {
    val e = new Throwable("foo")
    val tle = DelayedRebalanceEffect(throw e).catchAll(e => log(e.getMessage))
    tle.run()
    logged === Seq("foo")
  }

  trait ctx extends Scope {
    var logged = Vector.empty[String]
    def log(s: String) = logged = logged :+ s
  }

}
