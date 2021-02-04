package com.wixpress.dst.greyhound.core.consumer

import java.time.Clock

import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.CommittedMissingOffsets
import com.wixpress.dst.greyhound.core.{ClientId, Group, Offset}
import com.wixpress.dst.greyhound.core.consumer.domain.TopicPartition
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import zio.blocking.Blocking
import zio.{URIO, ZIO}
import zio.duration._

class MissingOffsetsCommitter(clientId: ClientId,
                              group: Group,
                              offsetOperations: UnsafeOffsetOperations,
                              timeout: zio.duration.Duration,
                              reporter: GreyhoundMetric => Unit,
                              clock: Clock = Clock.systemUTC
                             ) {
  def commitMissingOffsets(partitions: Set[TopicPartition]): Unit = {
    withReporting(partitions) {
      val committed = offsetOperations.committed(partitions, timeout)
      val notCommitted = partitions -- committed.keySet
      val positions = notCommitted.map(tp => tp -> offsetOperations.position(tp, timeout)).toMap
      if (positions.nonEmpty) {
        offsetOperations.commit(positions, timeout)
      }
      positions
    }
  }

  private def withReporting(partitions: Set[TopicPartition])(operation: => Map[TopicPartition, Offset]): Unit = {
    val start = clock.millis
    try {
      val positions = operation
      reporter(CommittedMissingOffsets(clientId, group, partitions, positions, Duration.fromMillis(clock.millis - start), None))
    } catch {
      case e: Throwable =>
        reporter(CommittedMissingOffsets(clientId, group, partitions, Map.empty, Duration.fromMillis(clock.millis - start), Some(e)))
    }
  }
}

object MissingOffsetsCommitter {
  def make(clientId: ClientId,
           group: Group,
           offsetOperations: UnsafeOffsetOperations,
           timeout: zio.duration.Duration,
           clock: Clock = Clock.systemUTC
          ): URIO[Blocking with GreyhoundMetrics, MissingOffsetsCommitter] = for {
    metrics <- ZIO.environment[GreyhoundMetrics].map(_.get)
    runtime <- ZIO.runtime[Blocking]
  } yield
    new MissingOffsetsCommitter(
      clientId,
      group,
      offsetOperations,
      timeout,
      m => runtime.unsafeRunTask(metrics.report(m)),
      clock
    )
}

