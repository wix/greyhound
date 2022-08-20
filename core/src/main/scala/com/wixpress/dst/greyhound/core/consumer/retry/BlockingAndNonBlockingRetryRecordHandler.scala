package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.{Trace, ZIO}

trait BlockingAndNonBlockingRetryRecordHandler[K, V, R] {
  def handle(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[GreyhoundMetrics with R, Nothing, Any]
}

private[retry] object BlockingAndNonBlockingRetryRecordHandler {
  def apply[V, K, R](
    group: Group,
    blockingHandler: BlockingRetryRecordHandler[V, K, R],
    nonBlockingHandler: NonBlockingRetryRecordHandler[V, K, R]
  ): BlockingAndNonBlockingRetryRecordHandler[K, V, R] = new BlockingAndNonBlockingRetryRecordHandler[K, V, R] {
    override def handle(record: ConsumerRecord[K, V]) (implicit trace: Trace): ZIO[GreyhoundMetrics with R, Nothing, Any] = {
      val value = blockingHandler.handle(record)
      value.flatMap(result =>
        ZIO.when(!result.lastHandleSucceeded) {
          nonBlockingHandlerAfterBlockingFailed(record)
        }
      )
    }

    private def nonBlockingHandlerAfterBlockingFailed(
      record: ConsumerRecord[K, V]
    ): ZIO[GreyhoundMetrics with R, Nothing, Any] = {
      if (nonBlockingHandler.isHandlingRetryTopicMessage(group, record)) {
        nonBlockingHandler.handle(record)
      } else {
        nonBlockingHandler.handleAfterBlockingFailed(record)
      }
    }
  }
}
