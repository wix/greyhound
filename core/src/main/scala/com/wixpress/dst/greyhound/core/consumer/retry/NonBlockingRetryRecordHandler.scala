package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.producer.Producer
import zio.blocking.Blocking
import zio.clock.{Clock, sleep}
import zio.duration._
import zio.{Chunk, ZIO}

trait NonBlockingRetryRecordHandler[V, K, R] {
  def handle(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any]

  def isHandlingRetryTopicMessage(record: ConsumerRecord[K, V]): Boolean

  def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any]
}

private[retry] object NonBlockingRetryRecordHandler {
  def apply[V, K, E, R](handler: RecordHandler[R, E, K, V],
                        producer: Producer,
                        subscription: ConsumerSubscription,
                        evK: K <:< Chunk[Byte],
                        evV: V <:< Chunk[Byte],
                        nonBlockingRetryHelper: NonBlockingRetryHelper): NonBlockingRetryRecordHandler[V, K, R] = new NonBlockingRetryRecordHandler[V, K, R]{
    override def handle(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        ZIO.foreach_(retryAttempt)(_.sleep) *> handler.handle(record).catchAll {
          case Right(_: NonRetryableException) => ZIO.unit
          case error => maybeRetry(retryAttempt, error, record)
        }
      }
    }

    override def isHandlingRetryTopicMessage(record: ConsumerRecord[K, V]): Boolean = {
      val option = nonBlockingRetryHelper.retryTopicsFor("").-("").headOption
      option.exists(retryTopicTemplate => record.topic.contains(retryTopicTemplate))
    }

    override def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        maybeRetry(retryAttempt, BlockingHandlerFailed, record)
      }
    }

    private def maybeRetry[E](retryAttempt: Option[RetryAttempt], error: E, record: ConsumerRecord[K, V]): ZIO[Clock with Blocking with R, Nothing, Any] = {
      nonBlockingRetryHelper.retryDecision(retryAttempt, record.bimap(evK, evV), error, subscription) flatMap {
        case RetryWith(retryRecord) =>
          producer.produce(retryRecord).tapError(_ => sleep(5.seconds)).eventually.ignore
        case NoMoreRetries =>
          ZIO.unit //todo: report uncaught errors and producer failures
      }
    }
  }


}
