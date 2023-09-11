package com.wixpress.dst.greyhound.core.consumer.retry

import java.util.concurrent.TimeUnit
import com.wixpress.dst.greyhound.core.{Group, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.BlockingState.{Blocked, Blocking => InternalBlocking, IgnoringOnce}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{BlockingRetryHandlerInvocationFailed, DoneBlockingBeforeRetry, NoRetryOnNonRetryableFailure}
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
    awaitShutdown: TopicPartition => UIO[AwaitShutdown],
    interruptOnShutdown: Boolean
  ): BlockingRetryRecordHandler[V, K, R] = new BlockingRetryRecordHandler[V, K, R] {
    val blockingStateResolver = BlockingStateResolver(blockingState)
    case class PollResult(pollAgain: Boolean, blockHandling: Boolean) // TODO: switch to state enum

    override def handle(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[GreyhoundMetrics with R, Nothing, LastHandleResult] = {
      val topicPartition = TopicPartition(record.topic, record.partition)

      def pollBlockingStateWithSuspensions(
        record: ConsumerRecord[K, V],
        interval: Duration,
        start: Long
      ): URIO[GreyhoundMetrics, PollResult] = {
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

      def blockOnErrorFor(record: ConsumerRecord[K, V], interval: Duration) = {
        for {
          start            <- currentTime(TimeUnit.MILLISECONDS)
          continueBlocking <-
            if (interval.toMillis > 100L) {
              (if (interruptOnShutdown) {
                 awaitShutdown(record.topicPartition).flatMap(
                   _.interruptOnShutdown(
                     pollBlockingStateWithSuspensions(record, interval, start).repeatWhile(result => result.pollAgain).map(_.blockHandling)
                   )
                 )
               } else {
                 pollBlockingStateWithSuspensions(record, interval, start).repeatWhile(result => result.pollAgain).map(_.blockHandling)
               }).reporting(r => DoneBlockingBeforeRetry(record.topic, record.partition, record.offset, r.duration, r.failed))
            } else {
              for {
                shouldBlock <- blockingStateResolver.resolve(record)
                _           <- ZIO.when(shouldBlock)(ZIO.sleep(interval))
              } yield shouldBlock
            }
        } yield LastHandleResult(lastHandleSucceeded = false, shouldContinue = continueBlocking)
      }

      def handleAndMaybeBlockOnErrorFor(
        record: ConsumerRecord[K, V],
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
                report(BlockingRetryHandlerInvocationFailed(topicPartition, record.offset, error.toString)) *>
                  blockOnErrorFor(record, interval)
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
        val durations                                          = retryConfig.blockingBackoffs(record.topic)
        val durationsIncludingForInvocationWithNoErrorHandling = durations.map(Some(_)) :+ None
        for {
          result <- retryEvery(record, durationsIncludingForInvocationWithNoErrorHandling) { (rec, interval) =>
                      handleAndMaybeBlockOnErrorFor(rec, interval)
                    }
          _      <- maybeBackToStateBlocking
        } yield result
      }
    }
  }

  private def retryEvery[K, V, R, E](record: ConsumerRecord[K, V], as: Iterable[Option[Duration]])(
    f: (ConsumerRecord[K, V], Option[Duration]) => ZIO[R, E, LastHandleResult]
  )(implicit trace: Trace): ZIO[R, E, LastHandleResult] = {
    ZIO.succeed(as.iterator).flatMap { i =>
      def loop(retryAttempt: Option[RetryAttempt]): ZIO[R, E, LastHandleResult] =
        if (i.hasNext) {
          val nextDelay         = i.next
          val recordWithAttempt = retryAttempt.fold(record) { attempt =>
            record.copy(headers = record.headers ++ RetryAttempt.toHeaders(attempt))
          }
          f(recordWithAttempt, nextDelay).flatMap { result =>
            if (result.shouldContinue) Clock.instant.flatMap { now =>
              val nextAttempt = RetryAttempt(
                originalTopic = record.topic,
                attempt = retryAttempt.fold(0)(_.attempt + 1),
                submittedAt = now,
                backoff = nextDelay getOrElse Duration.Zero
              )
              loop(Some(nextAttempt))
            }
            else ZIO.succeed(result)
          }
        } else ZIO.succeed(LastHandleResult(lastHandleSucceeded = false, shouldContinue = false))

      loop(None)
    }
  }

  private def handleNonRetriable[K, V, E, R](record: ConsumerRecord[K, V], topicPartition: TopicPartition, cause: Exception) =
    report(NoRetryOnNonRetryableFailure(topicPartition, record.offset, cause))
      .as(LastHandleResult(lastHandleSucceeded = false, shouldContinue = false))
}
