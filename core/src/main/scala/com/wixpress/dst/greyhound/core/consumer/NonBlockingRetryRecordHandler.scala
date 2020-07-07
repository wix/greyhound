package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription}
import com.wixpress.dst.greyhound.core.consumer.retry.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.consumer.retry.{BlockingHandlerFailed, NonBlockingRetryPolicy, NonRetryableException, RetryAttempt}
import com.wixpress.dst.greyhound.core.producer.Producer
import zio.clock.{Clock, sleep}
import zio.{Chunk, ZIO}
import zio.duration._

trait NonBlockingRetryRecordHandler[V, K, R] {
  def handle(record: ConsumerRecord[K, V]): ZIO[Clock with R, Nothing, Any]

  def isHandlingRetryTopicMessage(record: ConsumerRecord[K, V]): Boolean

  def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with R, Nothing, Any]
}

object NonBlockingRetryRecordHandler {
  def apply[V, K, E, R](handler: RecordHandler[R, E, K, V],
                            producer: Producer,
                            subscription: ConsumerSubscription,
                            evK: K <:< Chunk[Byte],
                            evV: V <:< Chunk[Byte],
                            nonBlockingRetryPolicy: NonBlockingRetryPolicy): NonBlockingRetryRecordHandler[V, K, R] = new NonBlockingRetryRecordHandler[V, K, R]{
    override def handle(record: ConsumerRecord[K, V]): ZIO[Clock with R, Nothing, Any] = {
      nonBlockingRetryPolicy.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        ZIO.foreach_(retryAttempt)(_.sleep) *> handler.handle(record).catchAll {
          case Right(_: NonRetryableException) => ZIO.unit
          case error => maybeRetry(retryAttempt, error, record)
        }
      }
    }

    override def isHandlingRetryTopicMessage(record: ConsumerRecord[K, V]): Boolean = {
      val option = nonBlockingRetryPolicy.retryTopicsFor("").-("").headOption
      option.exists(retryTopicTemplate => record.topic.contains(retryTopicTemplate))
    }

    override def handleAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with R, Nothing, Any] = {
      nonBlockingRetryPolicy.retryAttempt(record.topic, record.headers, subscription).flatMap { retryAttempt =>
        maybeRetry(retryAttempt, BlockingHandlerFailed, record)
      }
    }

    private def maybeRetry[E](retryAttempt: Option[RetryAttempt], error: E, record: ConsumerRecord[K, V]): ZIO[Clock with R, Nothing, Any] = {
      nonBlockingRetryPolicy.retryDecision(retryAttempt, record.bimap(evK, evV), error, subscription) flatMap {
        case RetryWith(retryRecord) =>
          producer.produce(retryRecord).tapError(_ => sleep(5.seconds)).eventually.ignore
        case NoMoreRetries =>
          ZIO.unit //todo: report uncaught errors and producer failures
      }
    }
  }


}
