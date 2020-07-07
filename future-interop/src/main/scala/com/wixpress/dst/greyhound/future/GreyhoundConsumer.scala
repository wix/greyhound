package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.consumer.{OffsetReset, SerializationError, RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.{ClientId, Deserializer, Group, NonEmptySet}
import com.wixpress.dst.greyhound.future.GreyhoundConsumer.Handle
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Chunk, Task, ZIO}

import scala.concurrent.{ExecutionContext, Future}

case class GreyhoundConsumer[K, V](initialTopics: NonEmptySet[String],
                                   group: Group,
                                   clientId: ClientId,
                                   handle: Handle[K, V],
                                   keyDeserializer: Deserializer[K],
                                   valueDeserializer: Deserializer[V],
                                   offsetReset: OffsetReset = OffsetReset.Latest,
                                   errorHandler: ErrorHandler[K, V] = ErrorHandler.NoOp[K, V]) {

  def recordHandler: Handler[Env] =
    CoreRecordHandler(handle)
      .withErrorHandler { case (error, record) =>
        ZIO.fromFuture(_ => errorHandler.onUserException(error, record))
          .flatMap(_ => ZIO.fail(error))
      }
      .withDeserializers(keyDeserializer, valueDeserializer)
      .withErrorHandler { case (error, record) =>
        error match {
          case Left(serializationError) =>
            ZIO.fromFuture(_ => errorHandler.onSerializationError(serializationError, record)).catchAll(_ => ZIO.unit)
          case _ => ZIO.unit
        }
      }
}

object GreyhoundConsumer {
  type Handle[K, V] = ConsumerRecord[K, V] => Task[Any]

  def aRecordHandler[K, V](handler: RecordHandler[K, V]): Handle[K, V] =
    record => ZIO.fromFuture(ec => handler.handle(record)(ec))

  def aContextAwareRecordHandler[K, V, C](decoder: ContextDecoder[C])
                                         (handler: ContextAwareRecordHandler[K, V, C]): Handle[K, V] =
    record => decoder.decode(record).flatMap { context =>
      ZIO.fromFuture(ec => handler.handle(record)(context, ec))
    }
}

trait RecordHandler[K, V] {
  def handle(record: ConsumerRecord[K, V])(implicit ec: ExecutionContext): Future[Any]
}

trait ContextAwareRecordHandler[K, V, C] {
  def handle(record: ConsumerRecord[K, V])(implicit context: C, ec: ExecutionContext): Future[Any]
}

trait ErrorHandler[K, V] {
  self =>
  def onUserException(e: Throwable, record: ConsumerRecord[K, V]): Future[Unit]

  def onSerializationError(e: SerializationError, record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]): Future[Unit]

  def withSerializationErrorHandler(f: (SerializationError, ConsumerRecord[Chunk[Byte], Chunk[Byte]]) => Future[Unit]) =
    new ErrorHandler[K, V] {
      override def onUserException(e: Throwable, record: ConsumerRecord[K, V]) = self.onUserException(e, record)

      override def onSerializationError(e: SerializationError, record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]) =
        f(e, record)
    }
}

object ErrorHandler {
  def NoOp[K, V]: ErrorHandler[K, V] = anErrorHandler((_, _) => Future.successful(()))

  /**
   * @return a callback that will be called on handler errors.
   */
  def anErrorHandler[K, V](f: (Throwable, ConsumerRecord[K, V]) => Future[Unit]): ErrorHandler[K, V] =
    new ErrorHandler[K, V] {
      override def onUserException(e: Throwable, record: ConsumerRecord[K, V]): Future[Unit] =
        f(e, record)

      override def onSerializationError(e: SerializationError, record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]): Future[Unit] =
        Future.successful(())
    }
}