package com.wixpress.dst.greyhound.core

import java.util.Properties

import zio.duration.Duration

case class TopicConfig(name: Topic,
                       partitions: Int,
                       replicationFactor: Int,
                       cleanupPolicy: CleanupPolicy,
                       extraProperties: Map[String, String] = Map.empty) {

  def properties: Properties = {
    val props = new Properties
    propertiesMap.foreach {
      case (key, value) =>
        props.put(key, value)
    }
    props
  }

  def propertiesMap: Map[String, String] =
    (cleanupPolicy match {
      case CleanupPolicy.Delete(retention) =>
        Map(
          "retention.ms" -> retention.toMillis.toString,
          "cleanup.policy" -> "delete")

      case CleanupPolicy.Compact =>
        Map("cleanup.policy" -> "compact")
    }) ++ extraProperties
}

sealed trait CleanupPolicy

object CleanupPolicy {
  case class Delete(retention: Duration) extends CleanupPolicy
  case object Compact extends CleanupPolicy
}
