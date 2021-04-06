package com.wixpress.dst.greyhound.core

import org.apache.kafka.common

case class PartitionInfo(topic: String, partition: Int, replicaCount: Int)

object PartitionInfo {
  def apply(info: common.PartitionInfo): PartitionInfo =
    PartitionInfo(
      info.topic(),
      info.partition(),
      info.replicas().length
    )
}
