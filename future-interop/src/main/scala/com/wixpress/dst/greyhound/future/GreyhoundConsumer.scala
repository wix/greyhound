package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ConsumerRecord, RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.{Deserializer, Group, Topic}
import com.wixpress.dst.greyhound.future.GreyhoundConsumer.Handle
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Task, ZIO}

import scala.concurrent.{ExecutionContext, Future}

case class GreyhoundConsumer[K, V](topic: Topic,
                                   group: Group,
                                   handle: Handle[K, V],
                                   keyDeserializer: Deserializer[K],
                                   valueDeserializer: Deserializer[V]) {

  def recordHandler: Handler[Env] =
    CoreRecordHandler(topic)(handle)
      .withDeserializers(keyDeserializer, valueDeserializer)
      .withErrorHandler {
        // TODO handle errors
        case Left(serializationError) => ZIO.unit
        case Right(userError) => ZIO.unit
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
