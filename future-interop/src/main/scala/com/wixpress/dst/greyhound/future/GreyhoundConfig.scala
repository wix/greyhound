package com.wixpress.dst.greyhound.future

case class GreyhoundConfig(bootstrapServers: String,
                           runtime: GreyhoundRuntime = GreyhoundRuntime.Live)
