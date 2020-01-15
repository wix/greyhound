package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Offset
import zio.{UIO, ZIO}

trait PartitionsState { self =>

  def pause: UIO[Unit]

  def resume: UIO[Unit]

  def partitionsToPause: UIO[Map[TopicPartition, Offset]]

  def partitionsToResume: UIO[Set[TopicPartition]]

  def combine(other: PartitionsState): PartitionsState =
    new PartitionsState {
      override def pause: UIO[Unit] = self.pause *> other.pause

      override def resume: UIO[Unit] = self.resume *> other.resume

      override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
        (self.partitionsToPause zipWith other.partitionsToPause) { (a, b) =>
          a.foldLeft(b) {
            case (acc, (partition, offset)) =>
              val updated = acc.get(partition).foldLeft(offset)(_ min _)
              acc + (partition -> updated)
          }
        }

      override def partitionsToResume: UIO[Set[TopicPartition]] =
        (self.partitionsToResume zipWith other.partitionsToResume)(_ union _)
    }

}

trait EmptyPartitionsState extends PartitionsState {
  override def pause: UIO[Unit] = ZIO.unit

  override def resume: UIO[Unit] = ZIO.unit

  override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
    ZIO.succeed(Map.empty)

  override def partitionsToResume: UIO[Set[TopicPartition]] =
    ZIO.succeed(Set.empty)
}

object EmptyPartitionsState extends EmptyPartitionsState
