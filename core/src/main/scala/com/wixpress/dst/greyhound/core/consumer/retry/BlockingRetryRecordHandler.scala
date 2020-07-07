package com.wixpress.dst.greyhound.core.consumer.retry

import java.util.concurrent.TimeUnit

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{IgnoringOnce, Blocking => InternalBlocking}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.BlockingRetryOnHandlerFailed
import com.wixpress.dst.greyhound.core.consumer.retry.ZIOHelper.foreachWhile
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import zio._
import zio.clock.{Clock, currentTime}
import zio.duration._


trait BlockingRetryRecordHandler[V, K, R] {
  def handle(record: ConsumerRecord[K, V]): ZIO[Clock with GreyhoundMetrics with R, Nothing, LastHandleResult]
}

object BlockingRetryRecordHandler {
  def apply[R, E, V, K](handler: RecordHandler[R, E, K, V],
                        retryConfig: RetryConfig, blockingState: Ref[Map[BlockingTarget, BlockingState]],
                        nonBlockingRetryPolicy: NonBlockingRetryPolicy,
                        nonBlockingHandler: NonBlockingRetryRecordHandler[V, K, R]): BlockingRetryRecordHandler[V, K, R] = new BlockingRetryRecordHandler[V, K, R] {
    val blockingStateResolver = BlockingStateResolver(blockingState)
    case class PollResult(pollAgain: Boolean, blockHandling: Boolean) // TODO: switch to state enum

    override def handle(record: ConsumerRecord[K, V]): ZIO[Clock with GreyhoundMetrics with R, Nothing, LastHandleResult] = {
      val topicPartition = TopicPartition(record.topic, record.partition)

      def pollBlockingStateWithSuspensions(interval: Duration, start: Long): URIO[Clock with GreyhoundMetrics, PollResult] = {
        for {
          shouldBlock <- blockingStateResolver.shouldBlock(record)
          shouldPollAgain <- if (shouldBlock) {
            clock.sleep(100.milliseconds) *>
              currentTime(TimeUnit.MILLISECONDS).flatMap(end =>
                UIO(PollResult(pollAgain = end - start < interval.toMillis, blockHandling = true)))
          } else
            UIO(PollResult(pollAgain = false, blockHandling = false))
        } yield shouldPollAgain
      }

      def blockOnErrorFor(interval: Duration) = {
        for {
          start <- currentTime(TimeUnit.MILLISECONDS)
          continueBlocking <- if (interval.toMillis > 100L) {
            pollBlockingStateWithSuspensions(interval, start).doWhile(result => result.pollAgain).map(_.blockHandling)
          } else {
            for {
              shouldBlock <- blockingStateResolver.shouldBlock(record)
              _ <- ZIO.when(shouldBlock)(clock.sleep(interval))
            } yield shouldBlock
          }
        } yield LastHandleResult(lastHandleSucceeded = false, shouldContinue = continueBlocking)
      }

      def handleAndMaybeBlockOnErrorFor(interval: Option[Duration]): ZIO[Clock with R with GreyhoundMetrics, Nothing, LastHandleResult] = {
        (handler.handle(record).map(_ => LastHandleResult(lastHandleSucceeded = true, shouldContinue = false))).catchAll { _ =>
          interval.map { interval =>
            report(BlockingRetryOnHandlerFailed(topicPartition, record.offset)) *>
              blockOnErrorFor(interval)
          }.getOrElse(UIO(LastHandleResult(lastHandleSucceeded = false, shouldContinue = false)))
        }
      }

      def backToBlockingForIgnoringOnce = {
        blockingState.modify(state => state.get(TopicPartitionTarget(topicPartition)).map {
          case IgnoringOnce => ((), state.updated(TopicPartitionTarget(topicPartition), InternalBlocking))
          case _ => ((), state)
        }.getOrElse(((), state)))
      }

      if (nonBlockingHandler.isHandlingRetryTopicMessage(record)) {
        UIO(LastHandleResult(lastHandleSucceeded = false, shouldContinue = false))
      } else {
        val durationsIncludingForInvocationWithNoErrorHandling = retryConfig.blockingBackoffs().map(Some(_)) :+ None
        val result = foreachWhile(durationsIncludingForInvocationWithNoErrorHandling) { interval =>
          handleAndMaybeBlockOnErrorFor(interval)
        }
        backToBlockingForIgnoringOnce *> result
      }
    }
  }
}
