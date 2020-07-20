package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{IgnoringAll, IgnoringOnce, shouldBlockFrom, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import zio.{Ref, UIO, URIO, ZIO}

object BlockingStateResolver {
  def apply(blockingState: Ref[Map[BlockingTarget, BlockingState]]): BlockingStateResolver = {
    new BlockingStateResolver {
      override def resolve[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics, Boolean] = {
        val topicPartition = TopicPartition(record.topic, record.partition)

        for {
          mergedBlockingState <- blockingState.modify { state =>
            val topicPartitionBlockingState = state.getOrElse(TopicPartitionTarget(topicPartition),InternalBlocking)
            val topicBlockingState = state.getOrElse(TopicTarget(record.topic), InternalBlocking)
            val mergedBlockingState = topicBlockingState match {
              case IgnoringAll => IgnoringAll
              case IgnoringOnce => IgnoringOnce
              case _ => topicPartitionBlockingState
            }
            val shouldBlock = shouldBlockFrom(mergedBlockingState)
            val isBlockedAlready = mergedBlockingState match {
              case _:BlockingState.Blocked[K,V] => true
              case _ => false
            }
            val updatedState = if(shouldBlock && !isBlockedAlready) {
              state.updated(TopicPartitionTarget(topicPartition), BlockingState.Blocked(record.key, record.value, record.headers, topicPartition, record.offset))
            } else
              state
            (mergedBlockingState, updatedState)
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
  def resolve[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics, Boolean]
}