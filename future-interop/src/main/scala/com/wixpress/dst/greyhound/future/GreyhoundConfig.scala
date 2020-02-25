package com.wixpress.dst.greyhound.future

case class GreyhoundConfig(bootstrapServers: Set[String],
                           runtime: GreyhoundRuntime = GreyhoundRuntime.Live)
