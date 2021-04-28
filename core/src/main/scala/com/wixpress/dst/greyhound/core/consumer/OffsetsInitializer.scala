package com.wixpress.dst.greyhound.core.consumer

import java.time.Clock

import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.{CommittedMissingOffsets,CommittedMissingOffsetsFailed}
import com.wixpress.dst.greyhound.core.{ClientId, Group, Offset, TopicPartition}
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.blocking.Blocking
import zio.{URIO, ZIO}
import zio.duration._

/**
 * Called from `onPartitionsAssigned`.
 * Commits missing offsets to current position on assign - otherwise messages may be lost, in case of `OffsetReset.Latest`,
 * if a partition with no committed offset is revoked during processing.
 * Also supports seeking forward to some given initial offsets.
 */
class OffsetsInitializer(clientId: ClientId,
                         group: Group,
                         offsetOperations: UnsafeOffsetOperations,
                         timeout: zio.duration.Duration,
                         timeoutIfSeek: zio.duration.Duration,
                         reporter: GreyhoundMetric => Unit,
                         initialSeek: InitialOffsetsSeek,
                         clock: Clock = Clock.systemUTC) {
  def initializeOffsets(partitions: Set[TopicPartition]): Unit = {
    val hasSeek = initialSeek != InitialOffsetsSeek.default
    val effectiveTimeout = if (hasSeek) timeoutIfSeek else timeout

    withReporting(partitions, initialSeek, rethrow = hasSeek) {
      val committed = offsetOperations.committed(partitions, effectiveTimeout)
      val toOffsets: Map[TopicPartition, Offset] = initialSeek.seekOffsetsFor(partitions.map((_, None)).toMap ++ committed.mapValues(Some.apply))
      val notCommitted = partitions -- committed.keySet -- toOffsets.keySet
      val positions =
        notCommitted.map(tp => tp -> offsetOperations.position(tp, effectiveTimeout)).toMap ++
          toOffsets
      if (toOffsets.nonEmpty) {
        offsetOperations.seek(toOffsets)
      }
      if (positions.nonEmpty) {
        offsetOperations.commit(positions, effectiveTimeout)
      }
      positions
    }
  }

  private def withReporting(partitions: Set[TopicPartition],
                            initialOffsetsSeek: InitialOffsetsSeek,
                            rethrow: Boolean)
                           (operation: => Map[TopicPartition, Offset]): Unit = {
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

trait InitialOffsetsSeek {
  def seekOffsetsFor(currentCommittedOffsets: Map[TopicPartition, Option[Offset]]): Map[TopicPartition, Offset]
}

object InitialOffsetsSeek {
  val default: InitialOffsetsSeek = _ => Map.empty

  def setOffset(offsets: Map[TopicPartition, Offset]): InitialOffsetsSeek = _ => offsets
}

object OffsetsInitializer {
  def make(clientId: ClientId,
           group: Group,
           offsetOperations: UnsafeOffsetOperations,
           timeout: zio.duration.Duration,
           timeoutIfSeek: zio.duration.Duration,
           initialSeek: InitialOffsetsSeek,
           clock: Clock = Clock.systemUTC
          ): URIO[Blocking with GreyhoundMetrics, OffsetsInitializer] = for {
    metrics <- ZIO.environment[GreyhoundMetrics].map(_.get)
    runtime <- ZIO.runtime[Blocking]
  } yield
    new OffsetsInitializer(
      clientId,
      group,
      offsetOperations,
      timeout,
      timeoutIfSeek,
      m => runtime.unsafeRunTask(metrics.report(m)),
      initialSeek: InitialOffsetsSeek,
      clock
    )
}