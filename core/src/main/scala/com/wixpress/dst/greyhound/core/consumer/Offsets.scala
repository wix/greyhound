package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Offset
import zio.{Ref, UIO}

trait Offsets {
  def getAndClear: UIO[Map[TopicPartition, Offset]]

  def update(topicPartition: TopicPartition, offset: Offset): UIO[Unit]

  def update(record: ConsumerRecord[_, _]): UIO[Unit] =
    update(TopicPartition(record), record.offset)
}

object Offsets {
  def make: UIO[Offsets] =
    Ref.make(Map.empty[TopicPartition, Offset]).map { ref =>
      new Offsets {
        override def getAndClear: UIO[Map[TopicPartition, Offset]] =
          ref.modify(offsets => (offsets, Map.empty))

        override def update(topicPartition: TopicPartition, offset: Offset): UIO[Unit] =
          ref.update { offsets =>
            val updated = offsets.get(topicPartition).foldLeft(offset)(_ max _)
            offsets + (topicPartition -> updated)
          }.unit
      }
    }
}
