package com.wixpress.dst.greyhound.core.consumer

import java.util.concurrent.TimeUnit

import com.wixpress.dst.greyhound.core.consumer.BlockingState.{Blocking, IgnoringAll, IgnoringOnce, shouldBlockFrom}
import com.wixpress.dst.greyhound.core.consumer.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.RetryRecordHandlerMetric.{BlockingFor, BlockingIgnoredForAllFor, BlockingIgnoredOnceFor, BlockingRetryOnHandlerFailed}
import com.wixpress.dst.greyhound.core.consumer.ZIOHelper.foreachWhile
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.producer.Producer
import zio.clock.{Clock, currentTime, sleep}
import zio.duration._
import zio._

object RetryRecordHandler {
  /**
    * Return a handler with added retry behavior based on the provided `RetryPolicy`.
    * Upon failures,
    * 1. if non-blocking policy is chosen the `producer` will be used to send the failing records to designated
    * retry topics where the handling will be retried, after an optional delay. This
    * allows the handler to keep processing records in the original topic - however,
    * ordering will be lost for retried records!
    * 2. if blocking policy is chosen, the handling of the same message will be retried according to provided intervals
    * ordering is guaranteed
    */
  def withRetries[R2, R, E, K, V](handler: RecordHandler[R, E, K, V],
                                  retryPolicy: RetryPolicy, producer: Producer,
                                  subscription: ConsumerSubscription,
                                  blockingState: Ref[Map[TopicPartition, BlockingState]])
                                 (implicit evK: K <:< Chunk[Byte], evV: V <:< Chunk[Byte]): RecordHandler[R with R2 with Clock with GreyhoundMetrics, Nothing, K, V] =
    (record: ConsumerRecord[K, V]) => {
      if (retryPolicy.blockingIntervals.nonEmpty) {
        blockingHandler(handler, record, retryPolicy, blockingState)
      }
      else
        nonBlockingHandler(handler, producer, subscription, evK, evV, record, retryPolicy)
    }

  private def nonBlockingHandler[V, K, E, R, R2](handler: RecordHandler[R, E, K, V], producer: Producer, subscription: ConsumerSubscription, evK: K <:< Chunk[Byte], evV: V <:< Chunk[Byte], record: ConsumerRecord[K, V], policy: RetryPolicy) = {
    policy.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
      ZIO.foreach_(retryAttempt)(_.sleep) *> handler.handle(record).catchAll {
        case Right(_: NonRetryableException) => ZIO.unit
        case error => policy.retryDecision(retryAttempt, record.bimap(evK, evV), error, subscription) flatMap {
          case RetryWith(retryRecord) =>
            producer.produce(retryRecord).tapError(_ => sleep(5.seconds)).eventually
          case NoMoreRetries =>
            ZIO.unit //todo: report uncaught errors and producer failures
        }
      }
    }
  }

  private def blockingHandler[V, K, E, R, R2](handler: RecordHandler[R, E, K, V], record: ConsumerRecord[K, V], policy: RetryPolicy, blockingState: Ref[Map[TopicPartition, BlockingState]]): ZIO[Clock with R with GreyhoundMetrics, Nothing, Unit] = {
    case class PollResult(pollAgain: Boolean, blockHandling: Boolean) // TODO: switch to state enum
    val topicPartition = TopicPartition(record.topic, record.partition)

    def fetchShouldBlock = {
      blockingState.get.map(_.getOrElse(topicPartition, Blocking))
        .flatMap(blockingState => {
          UIO((blockingState,shouldBlockFrom(blockingState)))
        }).flatMap(pair => {
        val (blockingState, shouldBlock) = pair
        ZIO.when(!shouldBlock) {
          report(blockingState.metric(record))
        } *> UIO(shouldBlock)
      })
    }

    def pollBlockingStateWithSuspensions(interval: Duration, start: Long): URIO[Clock with GreyhoundMetrics, PollResult] = {
      for {
        shouldBlock <- fetchShouldBlock
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
            shouldBlock <- fetchShouldBlock
            _ <- ZIO.when(shouldBlock)(clock.sleep(interval))
          } yield shouldBlock
        }
      } yield continueBlocking
    }

    def handleAndMaybeBlockOnErrorFor(interval: Option[Duration]): ZIO[Clock with R with GreyhoundMetrics, Nothing, Boolean] = {
      (handler.handle(record).map(_ => false)).catchAll { _ =>
        interval.map { interval =>
          report(BlockingRetryOnHandlerFailed(topicPartition, record.offset)) *>
            blockOnErrorFor(interval)
        }.getOrElse(UIO(false))
      }
    }

    def backToBlockingForIgnoringOnce = {
      blockingState.modify(state => state.get(topicPartition).map {
        case IgnoringOnce => ((), state.updated(topicPartition, Blocking))
        case _ => ((), state)
      }.getOrElse(((), state)))
    }

    val durationsIncludingForInvocationWithNoErrorHandling = policy.blockingIntervals.map(Some(_)) :+ None
    foreachWhile(durationsIncludingForInvocationWithNoErrorHandling){ interval =>
      handleAndMaybeBlockOnErrorFor(interval)
    } *> backToBlockingForIgnoringOnce
  }
}

sealed trait RetryRecordHandlerMetric extends GreyhoundMetric

object RetryRecordHandlerMetric {

  case class BlockingFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingIgnoredForAllFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingIgnoredOnceFor(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric
  case class BlockingRetryOnHandlerFailed(partition: TopicPartition, offset: Long) extends RetryRecordHandlerMetric

}

sealed trait BlockingState {
  def metric[V, K](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric
}

object BlockingState {
  case object Blocking extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]) =
      BlockingFor(TopicPartition(record.topic, record.partition), record.offset)
  }
  case object IgnoringAll extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric =
      BlockingIgnoredForAllFor(TopicPartition(record.topic, record.partition), record.offset)
  }
  case object IgnoringOnce extends BlockingState {
    override def metric[V, K](record: ConsumerRecord[K, V]): RetryRecordHandlerMetric =
      BlockingIgnoredOnceFor(TopicPartition(record.topic, record.partition), record.offset)
  }

  def shouldBlockFrom(blockingState: BlockingState) = {
    blockingState match {
      case Blocking => true
      case IgnoringAll => false
      case IgnoringOnce => false
    }
  }
}

object ZIOHelper {
  def foreachWhile[R, E, A](as: Iterable[A])(f: A => ZIO[R, E, Boolean]): ZIO[R, E, Unit] =
    ZIO.effectTotal(as.iterator).flatMap { i =>
      def loop: ZIO[R, E, Unit] =
        if (i.hasNext) f(i.next).flatMap(result => if(result) loop else ZIO.unit)
        else ZIO.unit
      loop
    }
}