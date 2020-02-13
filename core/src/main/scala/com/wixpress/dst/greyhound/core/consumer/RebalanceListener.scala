package com.wixpress.dst.greyhound.core.consumer

import zio.{UIO, URIO, ZIO}

trait RebalanceListener[-R] {
  def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[R, Any]
  def onPartitionsAssigned(partitions: Set[TopicPartition]): URIO[R, Any]
}

object RebalanceListener {
  val Empty = new RebalanceListener[Any] {
    override def onPartitionsRevoked(partitions: Set[TopicPartition]): UIO[Any] = ZIO.unit
    override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] = ZIO.unit
  }
}
