package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.compression.GzipCompression
import com.wixpress.dst.greyhound.core.consumer.Gap.GAP_SEPARATOR
import com.wixpress.dst.greyhound.core.consumer.OffsetAndGaps.{GAPS_STRING_SEPARATOR, LAST_HANDLED_OFFSET_SEPARATOR}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordTopicPartition}
import com.wixpress.dst.greyhound.core.{Offset, OffsetAndMetadata, TopicPartition}
import zio._

import java.util.Base64
import scala.util.Try

trait OffsetsAndGaps {
  def init(committedOffsets: Map[TopicPartition, OffsetAndGaps]): UIO[Unit]

  def getCommittableAndClear: UIO[Map[TopicPartition, OffsetAndGaps]]

  def gapsForPartition(partition: TopicPartition): UIO[Seq[Gap]]

  def offsetsAndGapsForPartitions(partitions: Set[TopicPartition]): UIO[Map[TopicPartition, OffsetAndGaps]]

  def update(partition: TopicPartition, batch: Seq[Offset], prevCommittedOffset: Option[Offset]): UIO[Unit]

  def update(record: ConsumerRecord[_, _]): UIO[Unit] =
    update(RecordTopicPartition(record), Seq(record.offset), None)

  def update(records: Chunk[ConsumerRecord[_, _]]): UIO[Unit] = {
    val sortedBatch = records.sortBy(_.offset)
    update(RecordTopicPartition(sortedBatch.head), sortedBatch.map(_.offset) ++ Seq(sortedBatch.last.offset + 1), None)
  }

  def update(partition: TopicPartition, batch: Seq[Offset]): UIO[Unit] =
    update(partition, batch, None)

  def setCommittable(offsets: Map[TopicPartition, OffsetAndGaps]): UIO[Unit]

  def contains(partition: TopicPartition, offset: Offset): UIO[Boolean]
}

object OffsetsAndGaps {
  def make: UIO[OffsetsAndGaps] =
    Ref.make(Map.empty[TopicPartition, OffsetAndGaps]).map { ref =>
      new OffsetsAndGaps {
        override def init(committedOffsets: Map[TopicPartition, OffsetAndGaps]): UIO[Unit] =
          ref.update(_ => committedOffsets)

        override def getCommittableAndClear: UIO[Map[TopicPartition, OffsetAndGaps]] =
          ref.modify(offsetsAndGaps => {
            val committable = offsetsAndGaps.filter(_._2.committable)
            val updated     = offsetsAndGaps.mapValues(_.markCommitted)
            (committable, updated)
          })

        override def gapsForPartition(partition: TopicPartition): UIO[Seq[Gap]] =
          ref.get.map(_.get(partition).fold(Seq.empty[Gap])(_.gaps.sortBy(_.start)))

        override def offsetsAndGapsForPartitions(partitions: Set[TopicPartition]): UIO[Map[TopicPartition, OffsetAndGaps]] =
          ref.get.map(_.filterKeys(partitions.contains))

        override def update(partition: TopicPartition, batch: Seq[Offset], prevCommittedOffset: Option[Offset]): UIO[Unit] =
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

        override def setCommittable(offsets: Map[TopicPartition, OffsetAndGaps]): UIO[Unit] =
          ref.update { _ => offsets }

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

  def toOffsetsAndMetadata(offsetsAndGaps: Map[TopicPartition, OffsetAndGaps]): Map[TopicPartition, OffsetAndMetadata] =
    offsetsAndGaps.mapValues(offsetAndGaps => OffsetAndMetadata(offsetAndGaps.offset, offsetAndGaps.gapsString))

  def parseGapsString(rawOffsetAndGapsString: String): Option[OffsetAndGaps] = {
    val offsetAndGapsString             =
      if (rawOffsetAndGapsString.nonEmpty) {
        Try(new String(GzipCompression.decompress(Base64.getDecoder.decode(rawOffsetAndGapsString)).getOrElse(Array.empty))).getOrElse("")
      } else ""
    val lastHandledOffsetSeparatorIndex = offsetAndGapsString.indexOf(LAST_HANDLED_OFFSET_SEPARATOR)
    if (lastHandledOffsetSeparatorIndex < 0)
      None
    else {
      val lastHandledOffset = offsetAndGapsString.substring(0, lastHandledOffsetSeparatorIndex).toLong
      val gaps              = offsetAndGapsString
        .substring(lastHandledOffsetSeparatorIndex + 1)
        .split(GAPS_STRING_SEPARATOR)
        .map(_.split(GAP_SEPARATOR))
        .collect { case Array(start, end) => Gap(start.toLong, end.toLong) }
        .toSeq
        .sortBy(_.start)
      Some(OffsetAndGaps(lastHandledOffset, gaps))
    }
  }

  private def firstGapOffset(gapsString: String): Option[Offset] = {
    val maybeOffsetAndGaps = parseGapsString(gapsString)
    maybeOffsetAndGaps match {
      case Some(offsetAndGaps) if offsetAndGaps.gaps.nonEmpty => Some(offsetAndGaps.gaps.minBy(_.start).start)
      case _                                                  => None
    }
  }

  def gapsSmallestOffsets(offsets: Map[TopicPartition, Option[OffsetAndMetadata]]): Map[TopicPartition, OffsetAndMetadata] =
    offsets
      .collect { case (tp, Some(om)) => tp -> om }
      .map(tpom => tpom._1 -> (firstGapOffset(tpom._2.metadata), tpom._2.metadata))
      .collect { case (tp, (Some(offset), metadata)) => tp -> OffsetAndMetadata(offset, metadata) }
}

case class Gap(start: Offset, end: Offset) {
  def contains(offset: Offset): Boolean = start <= offset && offset <= end

  def size: Long = end - start + 1

  override def toString: String = s"$start$GAP_SEPARATOR$end"
}

object Gap {
  val GAP_SEPARATOR = "_"
}

case class OffsetAndGaps(offset: Offset, gaps: Seq[Gap], committable: Boolean = true) {
  def contains(offset: Offset): Boolean = gaps.exists(_.contains(offset))

  def markCommitted: OffsetAndGaps = copy(committable = false)

  def gapsString: String = {
    val plainGapsString = s"${offset.toString}${LAST_HANDLED_OFFSET_SEPARATOR}${gaps.sortBy(_.start).mkString(GAPS_STRING_SEPARATOR)}"
    Base64.getEncoder.encodeToString(GzipCompression.compress(plainGapsString.getBytes()))
  }

  def plainGapsString: String = s"${offset.toString}${LAST_HANDLED_OFFSET_SEPARATOR}${gaps.sortBy(_.start).mkString(GAPS_STRING_SEPARATOR)}"
}

object OffsetAndGaps {
  val GAPS_STRING_SEPARATOR         = "$"
  val LAST_HANDLED_OFFSET_SEPARATOR = "#"

  def apply(offset: Offset): OffsetAndGaps = OffsetAndGaps(offset, Seq.empty[Gap])

  def apply(offset: Offset, committable: Boolean): OffsetAndGaps = OffsetAndGaps(offset, Seq.empty[Gap], committable)

  def gapsSize(gaps: Map[TopicPartition, OffsetAndGaps]): Int =
    gaps.values.map(_.gaps.size).sum
}
