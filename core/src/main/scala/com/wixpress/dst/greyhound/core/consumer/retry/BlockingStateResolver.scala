package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{Blocked, IgnoringAll, IgnoringOnce, shouldBlockFrom, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import zio._
import zio.blocking.Blocking

object BlockingStateResolver {
  def apply(blockingStateRef: Ref[Map[BlockingTarget, BlockingState]]): BlockingStateResolver = {
    new BlockingStateResolver {
      override def resolve[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics with Blocking, Boolean] = {
        val topicPartition = TopicPartition(record.topic, record.partition)

        for {
          mergedBlockingState <- blockingStateRef.modify { state =>
            val topicPartitionBlockingState = state.get(TopicPartitionTarget(topicPartition))
            val topicBlockingState = state.getOrElse(TopicTarget(record.topic), InternalBlocking)
            val mergedBlockingState = topicPartitionBlockingState.map{
              case IgnoringAll => IgnoringAll
              case IgnoringOnce => IgnoringOnce
              case InternalBlocking => InternalBlocking
              case b: Blocked[V, K] => b
              case _ => InternalBlocking
            }.getOrElse(topicBlockingState)
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

      override def setBlockingState[R1](command: BlockingStateCommand): RIO[GreyhoundMetrics with Blocking, Unit] = {
        def handleIgnoreOnceRequest(topicPartition: TopicPartition) = {
          blockingStateRef.modify(prevState => {
            val previouslyBlocked = prevState.get(TopicPartitionTarget(topicPartition)).exists {
              case InternalBlocking => true
              case _: Blocked[Chunk[Byte], Chunk[Byte]] => true
              case _ => false
            }
            if (previouslyBlocked)
              (true, prevState.updated(TopicPartitionTarget(topicPartition), IgnoringOnce))
            else
              (false, prevState)
          }).flatMap(successful =>
            ZIO.when(!successful)(
              ZIO.fail(new RuntimeException("Request to IgnoreOnce when message is not blocked"))))

        }

        def updateTopicTargetAndPartitionTargets(topic: Topic, blockingState: BlockingState) = {
          blockingStateRef.modify(prevState => {
            val targetsToUpdate = prevState.filter(pair => pair._1 match {
              case TopicPartitionTarget(topicPartition) => topicPartition.topic == topic
              case _ => false
            })

            ((),prevState.updated(TopicTarget(topic), blockingState) ++ targetsToUpdate.mapValues(_ => blockingState))
          })
        }

        command match {
          case IgnoreOnceFor(topicPartition: TopicPartition) => handleIgnoreOnceRequest(topicPartition)
          case IgnoreAllFor(topicPartition: TopicPartition) => blockingStateRef.update(_.updated(TopicPartitionTarget(topicPartition), IgnoringAll))
          case BlockErrorsFor(topicPartition: TopicPartition) => blockingStateRef.update(_.updated(TopicPartitionTarget(topicPartition), InternalBlocking))
          case IgnoreAll(topic: Topic) => updateTopicTargetAndPartitionTargets(topic, IgnoringAll)
          case BlockErrors(topic: Topic) => updateTopicTargetAndPartitionTargets(topic, InternalBlocking)
          case _ => ZIO.fail(new RuntimeException(s"unfamiliar BlockingStateCommand: $command"))
        }
      }
    }
  }
}

trait BlockingStateResolver {
  def resolve[K, V](record: ConsumerRecord[K, V]): URIO[GreyhoundMetrics with Blocking, Boolean]
  def setBlockingState[R1](command: BlockingStateCommand): RIO[GreyhoundMetrics with Blocking, Unit]
}

sealed trait BlockingStateCommand

case class IgnoreOnceFor(topicPartition: TopicPartition) extends BlockingStateCommand
case class IgnoreAllFor(topicPartition: TopicPartition) extends BlockingStateCommand
case class BlockErrorsFor(topicPartition: TopicPartition) extends BlockingStateCommand
case class IgnoreAll(topic: Topic) extends BlockingStateCommand
case class BlockErrors(topic: Topic) extends BlockingStateCommand