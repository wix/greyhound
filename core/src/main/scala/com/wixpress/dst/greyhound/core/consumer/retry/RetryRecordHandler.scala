package com.wixpress.dst.greyhound.core.consumer.retry

import com.wixpress.dst.greyhound.core.Serdes.StringSerde
import com.wixpress.dst.greyhound.core.TopicPartition
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.ProducerR
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown
import zio._

object RetryRecordHandler {

  /**
   * Return a handler with added retry behavior based on the provided `RetryPolicy`. Upon failures,
   *   1. if non-blocking policy is chosen the `producer` will be used to send the failing records to designated retry topics where the
   *      handling will be retried, after an optional delay. This allows the handler to keep processing records in the original topic -
   *      however, ordering will be lost for retried records! 2. if blocking policy is chosen, the handling of the same message will be
   *      retried according to provided intervals ordering is guaranteed 3. if both policies are chosen, the blocking policy will be invoked
   *      first, and only if it fails the non-blocking policy will be invoked
   */
  def withRetries[R2, R, E, K, V](
    groupId: String,
    handler: RecordHandler[R, E, K, V],
    retryConfig: RetryConfig,
    producer: ProducerR[R],
    subscription: ConsumerSubscription,
    blockingState: Ref[Map[BlockingTarget, BlockingState]],
    nonBlockingRetryHelper: NonBlockingRetryHelper,
    awaitShutdown: TopicPartition => UIO[AwaitShutdown] = _ => ZIO.succeed(AwaitShutdown.never)(zio.Trace.empty),
    produceWithoutShutdown: Boolean = false
  )(
    implicit evK: K <:< Chunk[Byte],
    evV: V <:< Chunk[Byte]
  ): RecordHandler[R with R2 with GreyhoundMetrics, Nothing, K, V] = {

    val nonBlockingHandler            =
      NonBlockingRetryRecordHandler(
        handler,
        producer,
        retryConfig,
        subscription,
        nonBlockingRetryHelper,
        groupId,
        awaitShutdown,
        produceWithoutShutdown = produceWithoutShutdown
      )
    val blockingHandler               =
      BlockingRetryRecordHandler(
        groupId,
        handler,
        retryConfig,
        blockingState,
        nonBlockingHandler,
        awaitShutdown,
        interruptOnShutdown = !produceWithoutShutdown
      )
    val blockingAndNonBlockingHandler = BlockingAndNonBlockingRetryRecordHandler(groupId, blockingHandler, nonBlockingHandler)

    new RecordHandler[R with R2 with GreyhoundMetrics, Nothing, K, V] {
      override def handle(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[R with R2 with GreyhoundMetrics, Nothing, Any] =
        header(record, RetryHeader.OriginalTopic)
          .flatMap { originalTopic =>
            retryConfig.retryType(originalTopic.getOrElse(record.topic)) match {
              case BlockingFollowedByNonBlocking => blockingAndNonBlockingHandler.handle(record)
              case NonBlocking                   => nonBlockingHandler.handle(record)
              case Blocking                      => blockingHandler.handle(record)
              case NoRetries                     => handler.handle(record).ignore
            }
          }
    }
  }

  private def header[V, K, E, R, R2](record: ConsumerRecord[Any, Any], key: String)(implicit trace: Trace) =
    record.headers.get[String](key, StringSerde).catchAll(_ => ZIO.none)
}

case class LastHandleResult(lastHandleSucceeded: Boolean, shouldContinue: Boolean)
