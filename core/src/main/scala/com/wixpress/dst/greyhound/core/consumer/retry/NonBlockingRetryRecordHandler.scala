package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper._
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryRecordHandlerMetric.{DoneWaitingBeforeRetry, RetryProduceFailedWillRetry, WaitingBeforeRetry}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics._
import com.wixpress.dst.greyhound.core.producer.{ProducerR, ProducerRecord}
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown
import com.wixpress.dst.greyhound.core.{Group, TopicPartition}
import zio.Clock.sleep
import zio.Duration.fromScala
import zio.{Chunk, UIO, ZIO}

trait NonBlockingRetryRecordHandler[V, K, R] {
  def handle(record: ConsumerRecord[K, V]): ZIO[GreyhoundMetrics with R, Nothing, Any]

  def isHandlingRetryTopicMessage(group: Group, record: ConsumerRecord[K, V]): Boolean

  def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[GreyhoundMetrics with R, Nothing, Any]
}

private[retry] object NonBlockingRetryRecordHandler {
  def apply[V, K, E, R](
    handler: RecordHandler[R, E, K, V],
    producer: ProducerR[R],
    retryConfig: RetryConfig,
    subscription: ConsumerSubscription,
    nonBlockingRetryHelper: NonBlockingRetryHelper,
    groupId: Group,
    awaitShutdown: TopicPartition => UIO[AwaitShutdown],
    produceWithoutShutdown: Boolean
  )(implicit evK: K <:< Chunk[Byte], evV: V <:< Chunk[Byte]): NonBlockingRetryRecordHandler[V, K, R] =
    new NonBlockingRetryRecordHandler[V, K, R] {
      override def handle(record: ConsumerRecord[K, V]): ZIO[GreyhoundMetrics with R, Nothing, Any] = {
        RetryAttempt.extract(record.headers, record.topic, groupId, subscription, Some(retryConfig)).flatMap { retryAttempt =>
          maybeDelayRetry(record, retryAttempt) *>
            handler.handle(record).catchAll {
              case Right(_: NonRetriableException) => ZIO.unit
              case error                           => maybeRetry(retryAttempt, error, record)
            }
        }
      }.unit

      private def maybeDelayRetry(record: ConsumerRecord[K, V], retryAttempt: Option[RetryAttempt]) = {
        ZIO.foreachDiscard(retryAttempt)(delayRetry(record, awaitShutdown))
      }

      private def delayRetry(record: ConsumerRecord[_, _], awaitShutdown: TopicPartition => UIO[AwaitShutdown])(
        retryAttempt: RetryAttempt
      ) =
        zio.Random.nextInt.flatMap(correlationId =>
          report(
            WaitingBeforeRetry(record.topic, retryAttempt, record.partition, record.offset, correlationId)
          ) *>
            awaitShutdown(record.topicPartition)
              .flatMap(_.interruptOnShutdown(RetryUtil.sleep(retryAttempt)))
              .reporting(r =>
                DoneWaitingBeforeRetry(record.topic, record.partition, record.offset, retryAttempt, r.duration, r.failed, correlationId)
              )
        )

      override def isHandlingRetryTopicMessage(group: Group, record: ConsumerRecord[K, V]): Boolean = {
        subscription match {
          case _: TopicPattern =>
            record.topic.startsWith(patternRetryTopicPrefix(group))
          case _: Topics       =>
            record.topic.startsWith(fixedRetryTopicPrefix(originalTopic(record.topic, group), group))
        }
      }

      override def handleAfterBlockingFailed(
        record: ConsumerRecord[K, V]
      ): ZIO[GreyhoundMetrics with R, Nothing, Any] = {
        RetryAttempt.extract(record.headers, record.topic, groupId, subscription, Some(retryConfig)).flatMap { retryAttempt =>
          maybeRetry(retryAttempt, BlockingHandlerFailed, record)
        }
      }

      private def maybeRetry[E1](
        retryAttempt: Option[RetryAttempt],
        error: E1,
        record: ConsumerRecord[K, V]
      ): ZIO[GreyhoundMetrics with R, Nothing, Any] = {
        nonBlockingRetryHelper.retryDecision(retryAttempt, record.bimap(evK, evV), error, subscription) flatMap {
          case RetryWith(retryRecord) => producerToRetryTopic(retryAttempt, retryRecord, record)
          case NoMoreRetries          => ZIO.unit // todo: report uncaught errors and producer failures
        }
      }

      private def producerToRetryTopic(
        retryAttempt: Option[RetryAttempt],
        retryRecord: ProducerRecord[Chunk[Byte], Chunk[Byte]],
        record: ConsumerRecord[_, _]
      ) = {
        if (produceWithoutShutdown)
          produceToRetryInternal(retryAttempt, retryRecord, record)
        else
          awaitShutdown(record.topicPartition).flatMap(_.interruptOnShutdown(produceToRetryInternal(retryAttempt, retryRecord, record)))
      }

      private def produceToRetryInternal(
        retryAttempt: Option[RetryAttempt],
        retryRecord: ProducerRecord[Chunk[Byte], Chunk[Byte]],
        record: ConsumerRecord[_, _]
      ) = {
        retryConfig
          .produceEncryptor(record)
          .flatMap(_.encrypt(retryRecord))
          .flatMap(producer.produce)
          .tapError(e =>
            report(RetryProduceFailedWillRetry(retryRecord.topic, retryAttempt, retryConfig.produceRetryBackoff.toMillis, record, e)) *>
              sleep(fromScala(retryConfig.produceRetryBackoff))
          )
          .eventually
          .ignore
      }
    }
}
