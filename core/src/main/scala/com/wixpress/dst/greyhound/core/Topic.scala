package com.wixpress.dst.greyhound.core

import java.util.Properties

import zio.duration.Duration

case class TopicConfig(name: Topic,
                       partitions: Int,
                       replicationFactor: Int,
                       cleanupPolicy: CleanupPolicy) {

  def propertiesMap: Map[String, String] =
    cleanupPolicy match {
      case CleanupPolicy.Delete(retention) =>
        Map(
          "retention.ms" -> retention.toMillis.toString,
          "cleanup.policy" -> "delete")

      case CleanupPolicy.Compact =>
        Map("cleanup.policy" -> "compact")
    }

  def properties: Properties = {
    val props = new Properties
    propertiesMap.foreach(p => props.put(p._1, p._2))
    props
  }

}

sealed trait CleanupPolicy

object CleanupPolicy {
  case class Delete(retention: Duration) extends CleanupPolicy
  case object Compact extends CleanupPolicy
}
