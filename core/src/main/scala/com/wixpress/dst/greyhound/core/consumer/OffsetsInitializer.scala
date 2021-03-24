package com.wixpress.dst.greyhound.core.consumer

import java.time.Clock

import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.CommittedMissingOffsets
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
                         timeoutIfSeekForward: zio.duration.Duration,
                         reporter: GreyhoundMetric => Unit,
                         seekForwardTo: Map[TopicPartition, Offset],
                         clock: Clock = Clock.systemUTC) {
  def initializeOffsets(partitions: Set[TopicPartition]): Unit = {
    val hasSeekForward = seekForwardTo.exists { case (tp, _) =>  partitions(tp) }
    val effectiveTimeout = if(hasSeekForward) timeoutIfSeekForward else timeout
    withReporting(partitions, seekForwardTo, rethrow = hasSeekForward) {
      val committed = offsetOperations.committed(partitions, effectiveTimeout)
      val toSeekForward = seekForwardTo.filter { case (tp, pos) =>
        partitions(tp) && pos > committed.getOrElse(tp, 0L)
      }
      val notCommitted = partitions -- committed.keySet -- toSeekForward.keySet
      val positions =
        notCommitted.map(tp => tp -> offsetOperations.position(tp, effectiveTimeout)).toMap ++
        toSeekForward
      if(toSeekForward.nonEmpty) {
        offsetOperations.seek(toSeekForward)
      }
      if (positions.nonEmpty) {
        offsetOperations.commit(positions , effectiveTimeout)
      }
      positions
    }
  }

  private def withReporting(partitions: Set[TopicPartition], seekForwardTo: Map[TopicPartition, Offset], rethrow: Boolean)
                           (operation: => Map[TopicPartition, Offset]): Unit = {
    val start = clock.millis
    try {
      val positions = operation
      reporter(CommittedMissingOffsets(clientId, group, partitions, positions, Duration.fromMillis(clock.millis - start), seekForwardTo, None))
    } catch {
      case e: Throwable =>
        reporter(CommittedMissingOffsets(clientId, group, partitions, Map.empty, Duration.fromMillis(clock.millis - start), seekForwardTo, Some(e)))
        if(rethrow) throw e
    }
  }
}

object OffsetsInitializer {
  def make(clientId: ClientId,
           group: Group,
           offsetOperations: UnsafeOffsetOperations,
           timeout: zio.duration.Duration,
           timeoutIfSeekForward: zio.duration.Duration,
           seekForwardTo: Map[TopicPartition, Offset],
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
      timeoutIfSeekForward,
      m => runtime.unsafeRunTask(metrics.report(m)),
      seekForwardTo,
      clock
    )
}

