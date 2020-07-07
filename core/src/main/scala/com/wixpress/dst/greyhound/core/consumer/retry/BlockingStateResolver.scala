package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{IgnoringAll, IgnoringOnce, shouldBlockFrom, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import zio.{Ref, URIO, ZIO}

object BlockingStateResolver {
  def apply(blockingState: Ref[Map[BlockingTarget, BlockingState]]): BlockingStateResolver = {
    new BlockingStateResolver {
      override def shouldBlock[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics, Boolean] = {
        val topicPartition = TopicPartition(record.topic, record.partition)

        for {
          topicPartitionBlockingState <- blockingState.get.map(_.getOrElse(TopicPartitionTarget(topicPartition),InternalBlocking))
          topicBlockingState <- blockingState.get.map(_.getOrElse(TopicTarget(record.topic), InternalBlocking))
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