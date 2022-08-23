package com.wixpress.dst.greyhound.java

import java.util.concurrent.{CompletableFuture, Executor}

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord => CoreConsumerRecord, RecordHandler => CoreRecordHandler, SerializationError}
import com.wixpress.dst.greyhound.core.{Deserializer => CoreDeserializer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime
import com.wixpress.dst.greyhound.java.Convert.toScala
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import zio.{Semaphore, ZIO}

import scala.concurrent.Promise

object GreyhoundConsumer {
  def `with`[K >: AnyRef, V](
    initialTopic: String,
    group: String,
    handler: RecordHandler[K, V],
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ) = {
    new GreyhoundConsumer(
      initialTopic,
      group,
      handler,
      keyDeserializer,
      valueDeserializer,
      offsetReset = OffsetReset.Latest,
      errorHandler = ErrorHandler.NoOp,
      parallelism = 1,
      retryConfig = None
    )
  }
}

case class GreyhoundConsumer[K >: AnyRef, V] private (
  initialTopic: String,
  group: String,
  handler: RecordHandler[K, V],
  keyDeserializer: Deserializer[K],
  valueDeserializer: Deserializer[V],
  offsetReset: OffsetReset,
  errorHandler: ErrorHandler[K, V],
  parallelism: Int,
  retryConfig: Option[RetryConfig]
) {
  def withMaxParallelism(parallelism: Int) =
    copy(parallelism = parallelism)

  def withErrorHandler(errorHandler: ErrorHandler[K, V]) =
    copy(errorHandler = errorHandler)

  def withOffsetReset(offsetReset: OffsetReset) =
    copy(offsetReset = offsetReset)

  def withRetryConfig(retryConfig: RetryConfig) =
    copy(retryConfig = Option(retryConfig))

  private[greyhound] def recordHandler(executor: Executor, runtime: zio.Runtime[GreyhoundRuntime.Env]) = {
    val baseHandler = runtime.unsafeRun(Semaphore.make(parallelism).map { semaphore =>
      CoreRecordHandler { record: CoreConsumerRecord[K, V] =>
        semaphore.withPermit {
          ZIO.effectAsync[Any, Throwable, Unit] { cb =>
            val kafkaRecord =
              new ConsumerRecord(record.topic, record.partition, record.offset, record.key.orNull, record.value) // TODO headers

            handler
              .handle(kafkaRecord, executor)
              .handle[Unit] { (_, error) =>
                if (error != null) cb(ZIO.fail(error))
                else cb(ZIO.unit)
              }
          }
        }
      }
    })

    baseHandler
      .withErrorHandler {
        case (t, record) =>
          ZIO.fromFuture(_ => toScala(errorHandler.onUserException(t, record))).catchAll(_ => ZIO.unit) *> ZIO.fail(t)
      }
      .withDeserializers(CoreDeserializer(keyDeserializer), CoreDeserializer(valueDeserializer))
      .withErrorHandler {
        case (error, record) =>
          error match {
            case Left(serializationError) =>
              ZIO
                .fromFuture(_ => toScala(errorHandler.onSerializationError(serializationError, record.bimap(_.toArray, _.toArray))))
                .catchAll(_ => ZIO.unit) *> ZIO.fail(serializationError)
            case _                        => ZIO.fail(error)
          }
      }
  }
}

object Convert {
  def toScala[A](completableFuture: CompletableFuture[A]): scala.concurrent.Future[A] = {
    val promise = Promise[A]()

    completableFuture.whenComplete((value, exception) => {
      Option(exception) match {
        case Some(ex) => promise.tryFailure(ex)
        case None     => promise.trySuccess(value)
      }
    })

    promise.future
  }
}

trait ErrorHandler[K, V] {
  self =>
  def onUserException(e: Throwable, record: CoreConsumerRecord[K, V]): CompletableFuture[Unit]

  def onSerializationError(e: SerializationError, record: CoreConsumerRecord[Array[Byte], Array[Byte]]): CompletableFuture[Unit]
}

object ErrorHandler {
  def NoOp[K, V]: ErrorHandler[K, V] = new ErrorHandler[K, V] {
    override def onUserException(e: Throwable, record: CoreConsumerRecord[K, V]): CompletableFuture[Unit] =
      CompletableFuture.completedFuture(())

    override def onSerializationError(
      e: SerializationError,
      record: CoreConsumerRecord[Array[Byte], Array[Byte]]
    ): CompletableFuture[Unit] =
      CompletableFuture.completedFuture(())
  }
}
