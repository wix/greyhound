package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.consumer.{ConsumerRecord, RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.{Deserializer, Group, Topic}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Chunk, ZIO}

import scala.concurrent.{ExecutionContext, Future}

case class GreyhoundConsumer[K, V](topic: Topic,
                                   group: Group,
                                   handler: RecordHandler[K, V],
                                   keyDeserializer: Deserializer[K],
                                   valueDeserializer: Deserializer[V]) {

  def recordHandler: CoreRecordHandler[Env, Nothing, Chunk[Byte], Chunk[Byte]] = {
    val baseHandler = CoreRecordHandler(topic) { record: ConsumerRecord[K, V] =>
      ZIO.fromFuture(ec => handler.handle(record)(ec))
    }
    baseHandler
      .withDeserializers(keyDeserializer, valueDeserializer)
      .withErrorHandler {
        // TODO handle errors
        case Left(serializationError) => ZIO.unit
        case Right(userError) => ZIO.unit
      }
  }

}

trait RecordHandler[K, V] {
  def handle(record: ConsumerRecord[K, V])(implicit ec: ExecutionContext): Future[Any]
}
