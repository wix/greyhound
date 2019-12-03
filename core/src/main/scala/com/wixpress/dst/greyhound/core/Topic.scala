package com.wixpress.dst.greyhound.core

import java.util.Properties

import zio.duration.Duration

case class Topic[K, V](name: String) extends AnyVal

case class TopicConfig(name: String,
                       partitions: Int,
                       replicationFactor: Int,
                       cleanupPolicy: CleanupPolicy) {

  def properties: Properties = {
    val props = new Properties
    cleanupPolicy match {
      case CleanupPolicy.Delete(retention) =>
        props.setProperty("cleanup.policy", "delete")
        props.setProperty("retention.ms", retention.toMillis.toString)

      case CleanupPolicy.Compact =>
        props.setProperty("cleanup.policy", "compact")
    }
    props
  }

}

sealed trait CleanupPolicy


object CleanupPolicy {
  case class Delete(retention: Duration) extends CleanupPolicy
  case object Compact extends CleanupPolicy
}
