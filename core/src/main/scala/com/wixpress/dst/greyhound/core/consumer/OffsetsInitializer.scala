package com.wixpress.dst.greyhound.core.consumer

import java.time.Clock
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.{CommittedMissingOffsets, CommittedMissingOffsetsFailed}
import com.wixpress.dst.greyhound.core.{ClientId, Group, Offset, TopicPartition}
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}

import zio.{URIO, ZIO}

import zio._

/**
 * Called from `onPartitionsAssigned`. Commits missing offsets to current position on assign - otherwise messages may be lost, in case of
 * `OffsetReset.Latest`, if a partition with no committed offset is revoked during processing. Also supports seeking to some given initial
 * offsets based on provided [[InitialOffsetsSeek]]
 */
class OffsetsInitializer(
  clientId: ClientId,
  group: Group,
  offsetOperations: UnsafeOffsetOperations,
  timeout: zio.Duration,
  timeoutIfSeek: zio.Duration,
  reporter: GreyhoundMetric => Unit,
  initialSeek: InitialOffsetsSeek,
  rewindUncommittedOffsetsBy: Duration,
  clock: Clock = Clock.systemUTC,
  offsetResetIsEarliest: Boolean
) {
  def initializeOffsets(partitions: Set[TopicPartition]): Unit = {
    val hasSeek          = initialSeek != InitialOffsetsSeek.default
    val effectiveTimeout = if (hasSeek) timeoutIfSeek else timeout

    withReporting(partitions, rethrow = hasSeek) {
      val committed                            = offsetOperations.committed(partitions, effectiveTimeout)
      val beginning                            = offsetOperations.beginningOffsets(partitions, effectiveTimeout)
      val endOffsets                           = offsetOperations.endOffsets(partitions, effectiveTimeout)
      val PartitionActions(toOffsets, toPause) = calculateTargetOffsets(partitions, beginning, committed, endOffsets, effectiveTimeout)
      val notCommitted                         = partitions -- committed.keySet -- toOffsets.keySet

      offsetOperations.pause(toPause)
      val rewindUncommittedOffsets =
        if (offsetResetIsEarliest || notCommitted.isEmpty || rewindUncommittedOffsetsBy.isZero) Map.empty
        else
          offsetOperations
            .offsetsForTimes(notCommitted, clock.millis() - rewindUncommittedOffsetsBy.toMillis, effectiveTimeout)
            .map { case (tp, maybeRewindedOffset) => (tp, maybeRewindedOffset.orElse(endOffsets.get(tp)).getOrElse(0L)) }

      val positions =
        notCommitted.map(tp => tp -> offsetOperations.position(tp, effectiveTimeout)).toMap ++ toOffsets ++ rewindUncommittedOffsets

      if ((toOffsets ++ rewindUncommittedOffsets).nonEmpty) {
        offsetOperations.seek(toOffsets ++ rewindUncommittedOffsets)
      }

      if (positions.nonEmpty) {
        offsetOperations.commit(positions, effectiveTimeout)
      }
      positions
    }
  }

  case class PartitionActions(offsetSeeks: Map[TopicPartition, Offset], partitionsToPause: Set[TopicPartition])

  private def calculateTargetOffsets(
    partitions: Set[TopicPartition],
    beginning: Map[TopicPartition, Offset],
    committed: Map[TopicPartition, Offset],
    endOffsets: Map[TopicPartition, Offset],
    timeout: Duration
  ): PartitionActions = {
    val seekTo: Map[TopicPartition, SeekTo] = initialSeek.seekOffsetsFor(
      assignedPartitions = partitions,
      beginningOffsets = partitions.map((_, None)).toMap ++ beginning.mapValues(Some.apply),
      endOffsets = partitions.map((_, None)).toMap ++ endOffsets.mapValues(Some.apply),
      currentCommittedOffsets = partitions.map((_, None)).toMap ++ committed.mapValues(Some.apply)
    )
    val seekToOffsets                       = seekTo.collect { case (k, v: SeekTo.SeekToOffset) => k -> v.offset }
    val seekToEndPartitions                 = seekTo.collect { case (k, SeekTo.SeekToEnd) => k }.toSet
    val toPause                             = seekTo.collect { case (k, SeekTo.Pause) => k }
    val seekToEndOffsets                    = fetchEndOffsets(seekToEndPartitions, timeout)
    val toOffsets                           = seekToOffsets ++ seekToEndOffsets

    PartitionActions(offsetSeeks = toOffsets, partitionsToPause = toPause.toSet)
  }

  private def fetchEndOffsets(seekToEndPartitions: Set[TopicPartition], timeout: Duration) = {
    if (seekToEndPartitions.nonEmpty) {
      offsetOperations.endOffsets(seekToEndPartitions, timeout)
    } else {
      Map.empty
    }
  }

  private def withReporting(partitions: Set[TopicPartition], rethrow: Boolean)(operation: => Map[TopicPartition, Offset]): Unit = {
    val start = clock.millis

    try {
      val positions = operation
      reporter(CommittedMissingOffsets(clientId, group, partitions, positions, Duration.fromMillis(clock.millis - start), positions))
    } catch {
      case e: Throwable =>
        reporter(CommittedMissingOffsetsFailed(clientId, group, partitions, Map.empty, Duration.fromMillis(clock.millis - start), e))
        if (rethrow) throw e
    }
  }
}

/**
 * Strategy for initial seek based on currently committed offsets. `currentCommittedOffsets` will contain a key for every assigned
 * partition. If no committed offset the value will be [[None]].
 */
trait InitialOffsetsSeek {
  def seekOffsetsFor(
    assignedPartitions: Set[TopicPartition],
    beginningOffsets: Map[TopicPartition, Option[Offset]],
    endOffsets: Map[TopicPartition, Option[Offset]],
    currentCommittedOffsets: Map[TopicPartition, Option[Offset]]
  ): Map[TopicPartition, SeekTo]
}

sealed trait SeekTo

object SeekTo {
  case object SeekToEnd extends SeekTo

  case class SeekToOffset(offset: Offset) extends SeekTo

  case object Pause extends SeekTo
}

object InitialOffsetsSeek {
  val default: InitialOffsetsSeek = (_, _, _, _) => Map.empty

  def setOffset(offsets: Map[TopicPartition, SeekTo]): InitialOffsetsSeek = (_, _, _, _) => offsets

  def pauseAllBut(partition: TopicPartition, seekTo: SeekTo): InitialOffsetsSeek = (assigned, _, _, _) =>
    assigned.collect {
      case `partition` => partition -> seekTo
      case tp          => tp        -> SeekTo.Pause
    }.toMap

}

object OffsetsInitializer {
  def make(
    clientId: ClientId,
    group: Group,
    offsetOperations: UnsafeOffsetOperations,
    timeout: zio.Duration,
    timeoutIfSeek: zio.Duration,
    initialSeek: InitialOffsetsSeek,
    clock: Clock = Clock.systemUTC,
    rewindUncommittedOffsetsBy: Duration,
    offsetResetIsEarliest: Boolean
  )(implicit trace: Trace): URIO[GreyhoundMetrics, OffsetsInitializer] = for {
    metrics <- ZIO.environment[GreyhoundMetrics].map(_.get)
    runtime <- ZIO.runtime[Any]
  } yield new OffsetsInitializer(
    clientId,
    group,
    offsetOperations,
    timeout,
    timeoutIfSeek,
    m => zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(metrics.report(m)).getOrThrowFiberFailure() },
    initialSeek: InitialOffsetsSeek,
    rewindUncommittedOffsetsBy,
    clock,
    offsetResetIsEarliest
  )
}
