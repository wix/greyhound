package com.wixpress.dst.greyhound.core.consumer

import zio.{UIO, URIO, ZIO}

trait RebalanceListener[-R] { self =>
  def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[R, Any]
  def onPartitionsAssigned(partitions: Set[TopicPartition]): URIO[R, Any]

  def *>[R1] (other: RebalanceListener[R1]) = new RebalanceListener[R with R1] {
    override def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[R with R1, Any] =
      self.onPartitionsRevoked(partitions) *> other.onPartitionsRevoked(partitions)

    override def onPartitionsAssigned(partitions: Set[TopicPartition]): URIO[R with R1, Any] =
      self.onPartitionsAssigned(partitions) *> other.onPartitionsAssigned(partitions)
  }
}

object RebalanceListener {
  val Empty = new RebalanceListener[Any] {
    override def onPartitionsRevoked(partitions: Set[TopicPartition]): UIO[Any] = ZIO.unit
    override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] = ZIO.unit
  }
}
