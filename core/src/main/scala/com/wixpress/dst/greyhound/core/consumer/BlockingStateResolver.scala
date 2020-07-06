package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.BlockingState.{Blocking, IgnoringAll, IgnoringOnce, shouldBlockFrom}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import zio.{Ref, UIO, URIO, ZIO}

// TODO: move to 'retry' package
object BlockingStateResolver {
  def apply(blockingState: Ref[Map[BlockingTarget, BlockingState]]): BlockingStateResolver = {
    new BlockingStateResolver {
      override def shouldBlock[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics, Boolean] = {
        val topicPartition = TopicPartition(record.topic, record.partition)

        for {
          topicPartitionBlockingState <- blockingState.get.map(_.getOrElse(TopicPartitionTarget(topicPartition), Blocking))
          topicBlockingState <- blockingState.get.map(_.getOrElse(TopicTarget(record.topic), Blocking))
          mergedBlockingState = topicBlockingState match {
            case IgnoringAll => IgnoringAll
            case IgnoringOnce => IgnoringOnce
            case _ => topicPartitionBlockingState
          }
          shouldBlock = shouldBlockFrom(mergedBlockingState)
          _ <- ZIO.when(!shouldBlock) {
            report(mergedBlockingState.metric(record))
          }
        } yield shouldBlock
      }
    }
  }
}

trait BlockingStateResolver {
  def shouldBlock[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics, Boolean]
}