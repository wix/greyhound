package com.wixpress.dst.greyhound.java

import java.time.Duration
import java.util.concurrent.Executor

import com.wixpress.dst.greyhound.core.consumer.batched.BatchRetryConfig
import com.wixpress.dst.greyhound.core.consumer.{InitialOffsetsSeek, RebalanceListener, RecordConsumerConfig}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime

object GreyhoundBatchConsumer {
  def `with`[K >: AnyRef, V](
                              initialTopic: String,
                              group: String,
                              handler: BatchRecordHandler[K, V]
                            ) =
    new GreyhoundBatchConsumer(
      initialTopic,
      group,
      handler,
      clientId = RecordConsumerConfig.makeClientId,
      offsetReset = OffsetReset.Latest,
      extraProperties = Map.empty,
      userProvidedListener = RebalanceListener.Empty, // hide from api
      resubscribeTimeout = Duration.ofSeconds(30), // hide from api
      initialOffsetsSeek = InitialOffsetsSeek.default, // hide from api
      retryConfig = None
    )
}

case class GreyhoundBatchConsumer[K >: AnyRef, V](
                                                   initialTopic: String,
                                                   group: String,
                                                   handler: BatchRecordHandler[K, V],
                                                   clientId: String,
                                                   offsetReset: OffsetReset,
                                                   extraProperties: Map[String, String],
                                                   userProvidedListener: RebalanceListener[Any],
                                                   resubscribeTimeout: Duration,
                                                   initialOffsetsSeek: InitialOffsetsSeek,
                                                   retryConfig: Option[BatchRetryConfig]
                                                 ) {

  // todo implement
  private[greyhound] def batchRecordHandler(executor: Executor, runtime: zio.Runtime[GreyhoundRuntime.Env]) = ???
}
