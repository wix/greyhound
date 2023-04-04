package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.{Offset, TopicPartition}
import zio._

trait OffsetsAndGaps {
  def getCommittableAndClear: UIO[Map[TopicPartition, OffsetAndGaps]]

  def gapsForPartition(partition: TopicPartition): UIO[Seq[Gap]]

  def update(partition: TopicPartition, batch: Seq[Offset]): UIO[Unit]

  def contains(partition: TopicPartition, offset: Offset): UIO[Boolean]
}

object OffsetsAndGaps {
  def make: UIO[OffsetsAndGaps] =
    Ref.make(Map.empty[TopicPartition, OffsetAndGaps]).map { ref =>
      new OffsetsAndGaps {
        override def getCommittableAndClear: UIO[Map[TopicPartition, OffsetAndGaps]] =
          ref.modify(offsetsAndGaps => {
            val committable = offsetsAndGaps.filter(_._2.committable)
            val updated     = offsetsAndGaps.mapValues(_.markCommitted)
            (committable, updated)
          })

        override def gapsForPartition(partition: TopicPartition): UIO[Seq[Gap]] =
          ref.get.map(_.get(partition).fold(Seq.empty[Gap])(_.gaps.sortBy(_.start)))

        override def update(partition: TopicPartition, batch: Seq[Offset]): UIO[Unit] =
          ref.update { offsetsAndGaps =>
            val sortedBatch            = batch.sorted
            val maxBatchOffset         = sortedBatch.last
            val maybeOffsetAndGaps     = offsetsAndGaps.get(partition)
            val prevOffset             = maybeOffsetAndGaps.fold(-1L)(_.offset)
            val partitionOffsetAndGaps = maybeOffsetAndGaps.fold(OffsetAndGaps(maxBatchOffset))(identity)

            val newGaps = gapsInBatch(sortedBatch, prevOffset)

            val updatedGaps = updateGapsByOffsets(
              partitionOffsetAndGaps.gaps ++ newGaps,
              sortedBatch
            )

            offsetsAndGaps + (partition -> OffsetAndGaps(maxBatchOffset max prevOffset, updatedGaps))
          }.unit

        override def contains(partition: TopicPartition, offset: Offset): UIO[Boolean] =
          ref.get.map(_.get(partition).fold(false)(_.contains(offset)))

        private def gapsInBatch(batch: Seq[Offset], prevLastOffset: Offset): Seq[Gap] =
          batch.sorted
            .foldLeft(Seq.empty[Gap], prevLastOffset) {
              case ((gaps, lastOffset), offset) =>
                if (offset <= lastOffset) (gaps, lastOffset)
                else if (offset == lastOffset + 1) (gaps, offset)
                else {
                  val newGap = Gap(lastOffset + 1, offset - 1)
                  (newGap +: gaps, offset)
                }
            }
            ._1
            .reverse

        private def updateGapsByOffsets(gaps: Seq[Gap], offsets: Seq[Offset]): Seq[Gap] = {
          val gapsToOffsets = gaps.map(gap => gap -> offsets.filter(o => o >= gap.start && o <= gap.end)).toMap
          gapsToOffsets.flatMap {
            case (gap, offsets) =>
              if (offsets.isEmpty) Seq(gap)
              else if (offsets.size == (gap.size)) Seq.empty[Gap]
              else gapsInBatch(offsets ++ Seq(gap.start - 1, gap.end + 1), gap.start - 2)
          }.toSeq
        }
      }
    }
}

case class Gap(start: Offset, end: Offset) {
  def contains(offset: Offset): Boolean = start <= offset && offset <= end

  def size: Long = end - start + 1
}

case class OffsetAndGaps(offset: Offset, gaps: Seq[Gap], committable: Boolean = true) {
  def contains(offset: Offset): Boolean = gaps.exists(_.contains(offset))

  def markCommitted: OffsetAndGaps = copy(committable = false)
}

object OffsetAndGaps {
  def apply(offset: Offset): OffsetAndGaps = OffsetAndGaps(offset, Seq.empty[Gap])
}
