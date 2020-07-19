package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{IgnoringAll, IgnoringOnce, shouldBlockFrom, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import zio.{Ref, URIO, ZIO}

object BlockingStateResolver {
  def apply(blockingState: Ref[Map[BlockingTarget, BlockingState]]): BlockingStateResolver = {
    new BlockingStateResolver {
      override def resolve[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics, Boolean] = {
        val topicPartition = TopicPartition(record.topic, record.partition)

        for {
          state <- blockingState.get
          topicPartitionBlockingState = state.getOrElse(TopicPartitionTarget(topicPartition),InternalBlocking)
          maybeTopicTarget = state.get(TopicTarget(record.topic))
          isTopicTarget = maybeTopicTarget.isDefined
          topicBlockingState = maybeTopicTarget.getOrElse(InternalBlocking)
          mergedBlockingState = topicBlockingState match {
            case IgnoringAll => IgnoringAll
            case IgnoringOnce => IgnoringOnce
            case _ => topicPartitionBlockingState
          }
          shouldBlock = shouldBlockFrom(mergedBlockingState)
          _ <- ZIO.when(!shouldBlock) {
            report(mergedBlockingState.metric(record))
          }
          _ <- ZIO.when(shouldBlock && !isTopicTarget) {
              blockingState.update(map => map.updated(TopicPartitionTarget(topicPartition), BlockingState.Blocked(record.key, record.value, record.headers, topicPartition, record.offset)))
          }
        } yield shouldBlock
      }
    }
  }
}

trait BlockingStateResolver {
  def resolve[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics, Boolean]
}