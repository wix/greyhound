package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper._
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{DoneWaitingBeforeRetry, RetryProduceFailedWillRetry, WaitingBeforeRetry}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics._
import com.wixpress.dst.greyhound.core.producer.{ProducerR, ProducerRecord}
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown
import zio.blocking.Blocking
import zio.clock.{Clock, sleep}
import zio.duration.Duration.fromScala
import zio.{Chunk, ZIO}

trait NonBlockingRetryRecordHandler[V, K, R] {
  def handle(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with GreyhoundMetrics with R, Nothing, Any]

  def isHandlingRetryTopicMessage(group: Group, record: ConsumerRecord[K, V]): Boolean

  def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with GreyhoundMetrics with  R, Nothing, Any]
}

private[retry] object NonBlockingRetryRecordHandler {
  def apply[V, K, E, R](handler: RecordHandler[R, E, K, V],
                        producer: ProducerR[R],
                        retryConfig: RetryConfig,
                        subscription: ConsumerSubscription,
                        nonBlockingRetryHelper: NonBlockingRetryHelper,
                        consumerShutdown: AwaitShutdown)
                       (implicit evK: K <:< Chunk[Byte], evV: V <:< Chunk[Byte]): NonBlockingRetryRecordHandler[V, K, R] = new NonBlockingRetryRecordHandler[V, K, R] {
    override def handle(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with GreyhoundMetrics with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        ZIO.foreach_(retryAttempt)(delayRetry(record, consumerShutdown)) *> handler.handle(record).catchAll {
          case Right(_: NonRetriableException) => ZIO.unit
          case error => maybeRetry(retryAttempt, error, record)
        }
      }
    }

    private def delayRetry(record: ConsumerRecord[_, _], consumerShutdown: AwaitShutdown)(retryAttempt: RetryAttempt) =  {
      report(
        WaitingBeforeRetry(record.topic, retryAttempt)
      ) *>
        consumerShutdown.interruptOnShutdown(retryAttempt.sleep)
        .reporting(r => DoneWaitingBeforeRetry(record.topic, retryAttempt, r.duration, r.failed))
    }

    override def isHandlingRetryTopicMessage(group: Group, record: ConsumerRecord[K, V]): Boolean = {
      subscription match {
        case _: TopicPattern =>
          record.topic.startsWith(patternRetryTopicPrefix(group))
        case _: Topics =>
          record.topic.startsWith(fixedRetryTopicPrefix(originalTopic(record.topic, group), group))
      }
    }

    override def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with GreyhoundMetrics with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        maybeRetry(retryAttempt, BlockingHandlerFailed, record)
      }
    }

    private def maybeRetry[E1](retryAttempt: Option[RetryAttempt], error: E1, record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with GreyhoundMetrics with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryDecision(retryAttempt, record.bimap(evK, evV), error, subscription) flatMap {
        case RetryWith(retryRecord) => producerToRetryTopic(retryAttempt, retryRecord)
        case NoMoreRetries => ZIO.unit //todo: report uncaught errors and producer failures
      }
    }

    private def producerToRetryTopic[E1](retryAttempt: Option[RetryAttempt],
                                         retryRecord: ProducerRecord[Chunk[Byte], Chunk[Byte]]) = {
      consumerShutdown.interruptOnShutdown(
        producer.produce(retryRecord).tapError(
          e =>
            report(
              RetryProduceFailedWillRetry(
                retryRecord.topic,
                retryAttempt,
                retryConfig.produceRetryBackoff.toMillis,
                e)) *>
              sleep(fromScala(retryConfig.produceRetryBackoff))
        ).eventually.ignore
      )
    }
  }
}
