package com.wixpress.dst.greyhound.java

import java.util

import com.wixpress.dst.greyhound.future.GreyhoundRuntime

import scala.collection.JavaConverters._

class GreyhoundConfig(val bootstrapServers: Set[String],
                      val runtime: GreyhoundRuntime) {

  def this(bootstrapServers: util.Set[String]) =
    this(bootstrapServers.asScala.toSet, GreyhoundRuntime.Live)

}
