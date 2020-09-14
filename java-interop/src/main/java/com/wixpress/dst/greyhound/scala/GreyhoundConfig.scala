package com.wixpress.dst.greyhound.java

import java.util

import com.wixpress.dst.greyhound.future.GreyhoundRuntime

import scala.collection.JavaConverters._

class GreyhoundConfig(val bootstrapServers: String,
                      val runtime: GreyhoundRuntime) {

  def this(bootstrapServers: util.Set[String]) =
    this(bootstrapServers.asScala.toSet.mkString(","), GreyhoundRuntime.Live)

  def this(bootstrapServers: String) =
    this(bootstrapServers, GreyhoundRuntime.Live)

}
