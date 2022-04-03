package com.wixpress.dst.greyhound.core

import java.util.Properties

trait CommonGreyhoundConfig {
  def bootstrapServers: String
  def kafkaProps: Map[String, String]
  def properties: Properties = kafkaProps.foldLeft(new Properties) { case (p, (k, v)) => p.put(k, v); p }
  def kafkaAuthProperties: Map[String, String] =
    kafkaProps.filter { case (key, _) => key.startsWith("sasl.") || key.startsWith("security.") || key.startsWith("ssl.") }
}
