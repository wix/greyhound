package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Offset
import zio.{UIO, ZIO}

trait PartitionsState { self =>

  def partitionsToPause: UIO[Map[TopicPartition, Offset]]

  def partitionsToResume: UIO[Set[TopicPartition]]

  def combine(other: PartitionsState): PartitionsState =
    new PartitionsState {
      override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
        (self.partitionsToPause zipWith other.partitionsToPause)(_ ++ _)

      override def partitionsToResume: UIO[Set[TopicPartition]] =
        (self.partitionsToResume zipWith other.partitionsToResume)(_ union _)
    }

}

object PartitionsState {
  val Empty = new PartitionsState {
    override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
      ZIO.succeed(Map.empty)

    override def partitionsToResume: UIO[Set[TopicPartition]] =
      ZIO.succeed(Set.empty)
  }
}
