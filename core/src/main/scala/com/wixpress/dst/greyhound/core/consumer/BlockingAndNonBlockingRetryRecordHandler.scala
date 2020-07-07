package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.ZIO
import zio.clock.Clock

trait BlockingAndNonBlockingRetryRecordHandler[K, V, R] {
  def handle(record: ConsumerRecord[K, V]): ZIO[Clock with GreyhoundMetrics with R, Nothing, Any]
}

object BlockingAndNonBlockingRetryRecordHandler {
  def apply[V, K, R](blockingHandler: BlockingRetryRecordHandler[V, K, R],
                     nonBlockingHandler: NonBlockingRetryRecordHandler[V, K, R]): BlockingAndNonBlockingRetryRecordHandler[K, V, R] = new BlockingAndNonBlockingRetryRecordHandler[K, V, R] {
    override def handle(record: ConsumerRecord[K, V]): ZIO[Clock with GreyhoundMetrics with R, Nothing, Any] = {
      val value = blockingHandler.handle(record)
      value.flatMap(result =>
        ZIO.when(!result.lastHandleSucceeded) {
          nonBlockingHandlerAfterBlockingFailed(record)
        }
      )
    }

    private def nonBlockingHandlerAfterBlockingFailed(record: ConsumerRecord[K, V]): ZIO[Clock with GreyhoundMetrics with R, Nothing, Any] = {
      if (nonBlockingHandler.isHandlingRetryTopicMessage(record)) {
        nonBlockingHandler.handle(record)
      } else {
        nonBlockingHandler.handleAfterBlockingFailed(record)
      }
    }
  }
}