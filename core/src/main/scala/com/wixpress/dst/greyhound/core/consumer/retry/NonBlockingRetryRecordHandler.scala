package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper._
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.producer.ProducerR
import zio.blocking.Blocking
import zio.clock.{Clock, sleep}
import zio.duration._
import zio.{Chunk, ZIO,UIO}

trait NonBlockingRetryRecordHandler[V, K, R] {
  def handle(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any]

  def isHandlingRetryTopicMessage(group: Group, record: ConsumerRecord[K, V]): Boolean

  def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any]
}

private[retry] object NonBlockingRetryRecordHandler {
  def apply[V, K, E, R](handler: RecordHandler[R, E, K, V],
                        producer: ProducerR[R],
                        subscription: ConsumerSubscription,
                        evK: K <:< Chunk[Byte],
                        evV: V <:< Chunk[Byte],
                        nonBlockingRetryHelper: NonBlockingRetryHelper): NonBlockingRetryRecordHandler[V, K, R] = new NonBlockingRetryRecordHandler[V, K, R]{
    override def handle(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        ZIO.foreach_(retryAttempt)(_.sleep) *> handler.handle(record).catchAll {
          case Right(_: NonRetriableException) => ZIO.unit
          case error => maybeRetry(retryAttempt, error, record)
        }
      }
    }

    override def isHandlingRetryTopicMessage(group: Group, record: ConsumerRecord[K, V]): Boolean = {
      subscription match {
        case _: TopicPattern =>
          record.topic.startsWith(patternRetryTopicPrefix(group))
        case _: Topics =>
          record.topic.startsWith(fixedRetryTopicPrefix(originalTopic(record.topic, group), group))
      }
    }

    override def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        maybeRetry(retryAttempt, BlockingHandlerFailed, record)
      }
    }

    private def maybeRetry[E1](retryAttempt: Option[RetryAttempt], error: E1, record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryDecision(retryAttempt, record.bimap(evK, evV), error, subscription) flatMap {
        case RetryWith(retryRecord) =>
          producer.produce(retryRecord).tapError(_ => sleep(5.seconds)).eventually.ignore
        case NoMoreRetries =>
          ZIO.unit //todo: report uncaught errors and producer failures
      }
    }
  }
}
