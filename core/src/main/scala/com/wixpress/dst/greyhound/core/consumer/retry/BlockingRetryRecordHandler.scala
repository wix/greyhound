package com.wixpress.dst.greyhound.core.consumer.retry

import java.util.concurrent.TimeUnit
import com.wixpress.dst.greyhound.core.{Group, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{Blocked, Blocking => InternalBlocking, IgnoringOnce}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{BlockingRetryHandlerInvocationFailed, DoneBlockingBeforeRetry, NoRetryOnNonRetryableFailure}
import com.wixpress.dst.greyhound.core.consumer.retry.ZIOHelper.foreachWhile
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown
import zio._
import zio.Clock.currentTime

trait BlockingRetryRecordHandler[V, K, R] {
  def handle(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[GreyhoundMetrics with R, Nothing, LastHandleResult]
}

private[retry] object BlockingRetryRecordHandler {
  def apply[R, E, V, K](
    group: Group,
    handler: RecordHandler[R, E, K, V],
    retryConfig: RetryConfig,
    blockingState: Ref[Map[BlockingTarget, BlockingState]],
    nonBlockingHandler: NonBlockingRetryRecordHandler[V, K, R],
    awaitShutdown: TopicPartition => UIO[AwaitShutdown]
  ): BlockingRetryRecordHandler[V, K, R] = new BlockingRetryRecordHandler[V, K, R] {
    val blockingStateResolver = BlockingStateResolver(blockingState)
    case class PollResult(pollAgain: Boolean, blockHandling: Boolean) // TODO: switch to state enum

    override def handle(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[GreyhoundMetrics with R, Nothing, LastHandleResult] = {
      val topicPartition = TopicPartition(record.topic, record.partition)

      def pollBlockingStateWithSuspensions(interval: Duration, start: Long): URIO[GreyhoundMetrics, PollResult] = {
        for {
          shouldBlock     <- blockingStateResolver.resolve(record)
          shouldPollAgain <-
            if (shouldBlock) {
              ZIO.sleep(100.milliseconds) *>
                currentTime(TimeUnit.MILLISECONDS).map(end => PollResult(pollAgain = end - start < interval.toMillis, blockHandling = true))
            } else
              ZIO.succeed(PollResult(pollAgain = false, blockHandling = false))
        } yield shouldPollAgain
      }

      def blockOnErrorFor(interval: Duration) = {
        for {
          start            <- currentTime(TimeUnit.MILLISECONDS)
          continueBlocking <-
            if (interval.toMillis > 100L) {
              awaitShutdown(record.topicPartition).flatMap(
                _.interruptOnShutdown(
                  pollBlockingStateWithSuspensions(interval, start).repeatWhile(result => result.pollAgain).map(_.blockHandling)
                ).reporting(r => DoneBlockingBeforeRetry(record.topic, record.partition, record.offset, r.duration, r.failed))
              )
            } else {
              for {
                shouldBlock <- blockingStateResolver.resolve(record)
                _           <- ZIO.when(shouldBlock)(ZIO.sleep(interval))
              } yield shouldBlock
            }
        } yield LastHandleResult(lastHandleSucceeded = false, shouldContinue = continueBlocking)
      }

      def handleAndMaybeBlockOnErrorFor(
        interval: Option[Duration]
      ): ZIO[R with GreyhoundMetrics, Nothing, LastHandleResult] = {
        handler.handle(record).map(_ => LastHandleResult(lastHandleSucceeded = true, shouldContinue = false)).catchAll {
          case NonRetriableException(cause)        =>
            handleNonRetriable(record, topicPartition, cause)
          case Right(NonRetriableException(cause)) =>
            handleNonRetriable(record, topicPartition, cause)
          case error                               =>
            interval
              .map { interval =>
                report(BlockingRetryHandlerInvocationFailed(topicPartition, record.offset, error.toString)) *> blockOnErrorFor(interval)
              }
              .getOrElse(ZIO.succeed(LastHandleResult(lastHandleSucceeded = false, shouldContinue = false)))
        }
      }

      def maybeBackToStateBlocking =
        blockingState.modify(state =>
          state
            .get(TopicPartitionTarget(topicPartition))
            .map {
              case IgnoringOnce     => ((), state.updated(TopicPartitionTarget(topicPartition), InternalBlocking))
              case _: Blocked[V, K] => ((), state.updated(TopicPartitionTarget(topicPartition), InternalBlocking))
              case _                => ((), state)
            }
            .getOrElse(((), state))
        )

      if (nonBlockingHandler.isHandlingRetryTopicMessage(group, record)) {
        ZIO.succeed(LastHandleResult(lastHandleSucceeded = false, shouldContinue = false))
      } else {
        val durationsIncludingForInvocationWithNoErrorHandling = retryConfig.blockingBackoffs(record.topic)().map(Some(_)) :+ None
        for {
          result <- foreachWhile(durationsIncludingForInvocationWithNoErrorHandling) { interval => handleAndMaybeBlockOnErrorFor(interval) }
          _      <- maybeBackToStateBlocking
        } yield result
      }
    }
  }

  private def handleNonRetriable[K, V, E, R](record: ConsumerRecord[K, V], topicPartition: TopicPartition, cause: Exception) =
    report(NoRetryOnNonRetryableFailure(topicPartition, record.offset, cause))
      .as(LastHandleResult(lastHandleSucceeded = false, shouldContinue = false))
}
