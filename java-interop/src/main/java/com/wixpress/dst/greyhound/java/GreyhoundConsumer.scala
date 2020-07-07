package com.wixpress.dst.greyhound.java

import java.util.concurrent.{CompletableFuture, Executor}

import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{SerializationError, RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord => CoreConsumerRecord}
import com.wixpress.dst.greyhound.core.{Deserializer => CoreDeserializer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import zio.ZIO
import Convert.toScala

import scala.concurrent.Promise

class GreyhoundConsumer[K >: AnyRef, V](val initialTopic: String,
                                        val group: String,
                                        val handler: RecordHandler[K, V],
                                        val keyDeserializer: Deserializer[K],
                                        val valueDeserializer: Deserializer[V],
                                        val offsetReset: OffsetReset,
                                        val errorHandler: ErrorHandler[K, V]) {

  def recordHandler(executor: Executor): Handler[Env] = {
    val baseHandler = CoreRecordHandler { record: CoreConsumerRecord[K, V] =>
      ZIO.effectAsync[Any, Throwable, Unit] { cb =>
        val kafkaRecord = new ConsumerRecord(
          record.topic,
          record.partition,
          record.offset,
          record.key.orNull,
          record.value) // TODO headers

        handler
          .handle(kafkaRecord, executor)
          .handle[Unit] { (_, error) =>
            if (error != null) cb(ZIO.fail(error))
            else cb(ZIO.unit)
          }
      }
    }
    baseHandler
      .withErrorHandler { case (t, record) =>
        ZIO.fromFuture(_ => toScala(errorHandler.onUserException(t, record))).catchAll(_ => ZIO.unit)
      }

      .withDeserializers(CoreDeserializer(keyDeserializer), CoreDeserializer(valueDeserializer))
      .withErrorHandler {
        case (error, record) => error match {
          case Left(serializationError) =>
            ZIO.fromFuture(_ => toScala(
              errorHandler.onSerializationError(serializationError, record.bimap(_.toArray, _.toArray))))
              .catchAll(_ => ZIO.unit)
          case _ => ZIO.unit
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
        case None => promise.trySuccess(value)
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

    override def onSerializationError(e: SerializationError, record: CoreConsumerRecord[Array[Byte], Array[Byte]]): CompletableFuture[Unit] =
      CompletableFuture.completedFuture(())
  }
}