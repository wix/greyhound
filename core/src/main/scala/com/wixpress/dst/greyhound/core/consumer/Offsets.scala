package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.{Offset, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordTopicPartition}
import zio.{Ref, UIO, ZIO}

trait Offsets {
  def committable: UIO[Map[TopicPartition, Offset]]

  def update(partition: TopicPartition, offset: Offset): UIO[Unit]

  def update(offsets: Map[TopicPartition, Offset]): UIO[Unit] = {
    ZIO.foreach_(offsets){ case (partition, offset) => update(partition, offset)}
  }

  def update(record: ConsumerRecord[_, _]): UIO[Unit] =
    update(RecordTopicPartition(record), record.offset + 1)

  def update(records: Seq[ConsumerRecord[_, _]]): UIO[Unit] = {
    val offsets = records.groupBy(RecordTopicPartition(_)).mapValues(_.maxBy(_.offset).offset + 1).toMap
    update(offsets)
  }
}

object Offsets {
  def make: UIO[Offsets] =
    Ref.make(Map.empty[TopicPartition, Offset]).map { ref =>
      new Offsets {
        override def committable: UIO[Map[TopicPartition, Offset]] =
          ref.modify(offsets => (offsets, Map.empty))

        override def update(partition: TopicPartition, offset: Offset): UIO[Unit] =
          ref.update { offsets =>
            val updatedOffset = offsets.get(partition).foldLeft(offset)(_ max _)
            offsets + (partition -> updatedOffset)
          }.unit
      }
    }
}
