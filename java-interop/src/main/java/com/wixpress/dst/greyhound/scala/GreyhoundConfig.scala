package com.wixpress.dst.greyhound.java

import java.util

import com.wixpress.dst.greyhound.future.GreyhoundRuntime

import scala.collection.JavaConverters._

class GreyhoundConfig(val bootstrapServers: String, val extraProperties: Map[String, String], val runtime: GreyhoundRuntime) {

  def this(bootstrapServers: util.Set[String]) =
    this(bootstrapServers.asScala.toSet.mkString(","), Map.empty, GreyhoundRuntime.Live)

  def this(bootstrapServers: util.Set[String], extraProperties: java.util.Map[String, String]) =
    this(bootstrapServers.asScala.toSet.mkString(","), extraProperties.asScala.toMap, GreyhoundRuntime.Live)

  def this(bootstrapServers: String) =
    this(bootstrapServers, Map.empty, GreyhoundRuntime.Live)

  def this(bootstrapServers: String, extraProperties: java.util.Map[String, String]) =
    this(bootstrapServers, extraProperties.asScala.toMap, GreyhoundRuntime.Live)
}
