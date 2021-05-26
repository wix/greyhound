package com.wixpress.dst.greyhound.java

import java.time.Duration
import java.util.concurrent.Executor

import com.wixpress.dst.greyhound.core.consumer.batched.{BatchConsumer, BatchRetryConfig}
import com.wixpress.dst.greyhound.core.consumer.domain.{HandleError, BatchRecordHandler => CoreBatchRecordHandler, ConsumerRecordBatch => CoreConsumerRecordBatch}
import com.wixpress.dst.greyhound.core.consumer.{InitialOffsetsSeek, RebalanceListener, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.{Deserializer => CoreDeserializer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime
import org.apache.kafka.common.serialization.Deserializer
import zio.{Chunk, ZIO}

object GreyhoundBatchConsumer {
  def `with`[K >: AnyRef, V](
                              initialTopic: String,
                              group: String,
                              handler: BatchRecordHandler[K, V],
                              keyDeserializer: Deserializer[K],
                              valueDeserializer: Deserializer[V],
                            ) =
    new GreyhoundBatchConsumer(
      initialTopic,
      group,
      handler,
      clientId = RecordConsumerConfig.makeClientId,
      offsetReset = OffsetReset.Latest,
      userProvidedListener = RebalanceListener.Empty, // hide from api
      resubscribeTimeout = Duration.ofSeconds(30), // hide from api
      initialOffsetsSeek = InitialOffsetsSeek.default, // hide from api
      retryConfig = None,
      keyDeserializer = keyDeserializer,
      valueDeserializer = valueDeserializer
    )
}

case class GreyhoundBatchConsumer[K >: AnyRef, V](
                                                   initialTopic: String,
                                                   group: String,
                                                   handler: BatchRecordHandler[K, V],
                                                   clientId: String,
                                                   offsetReset: OffsetReset,
                                                   keyDeserializer: Deserializer[K],
                                                   valueDeserializer: Deserializer[V],
                                                   userProvidedListener: RebalanceListener[Any],
                                                   resubscribeTimeout: Duration,
                                                   initialOffsetsSeek: InitialOffsetsSeek,
                                                   retryConfig: Option[BatchRetryConfig]
                                                 ) {
  private[greyhound] def batchRecordHandler(executor: Executor, runtime: zio.Runtime[GreyhoundRuntime.Env]):
  CoreBatchRecordHandler[Any with BatchConsumer.Env, Any, Chunk[Byte], Chunk[Byte]] = {
    val baseHandler: CoreBatchRecordHandler[Any, Throwable, K, V] = CoreBatchRecordHandler {
      records: CoreConsumerRecordBatch[K, V] =>
        ZIO.effectAsync[Any, HandleError[Throwable], Unit] { cb =>
          handler
            .handle(records, executor)
            .handle[Unit] { (_, error) =>
              if (error != null) cb(ZIO.fail(error).mapError(HandleError(_)))
              else cb(ZIO.unit)
            }
        }
    }
    baseHandler
      .withDeserializers(CoreDeserializer(keyDeserializer), CoreDeserializer(valueDeserializer))
  }

  def withResubscribeTimeout(seconds: Int) =
    copy(resubscribeTimeout = Duration.ofSeconds(seconds))

  def withOffsetReset(offsetReset: OffsetReset) =
    copy(offsetReset = offsetReset)

}
