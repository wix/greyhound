package com.wixpress.dst.greyhound.java

import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ConsumerRecord => CoreConsumerRecord, RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.{Deserializer => CoreDeserializer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer
import zio.ZIO

class GreyhoundConsumer[K, V](val topic: String,
                              val group: String,
                              val handler: RecordHandler[K, V],
                              val keyDeserializer: Deserializer[K],
                              val valueDeserializer: Deserializer[V]) {

  def recordHandler: Handler[Env] = {
    val baseHandler = CoreRecordHandler(topic) { record: CoreConsumerRecord[K, V] =>
      ZIO.effectAsync[Any, Throwable, Unit] { cb =>
        val kafkaRecord = new ConsumerRecord(
          record.topic,
          record.partition,
          record.offset,
          record.key.get, // TODO .get
          record.value) // TODO headers

        handler
          .handle(kafkaRecord)
          .handle[Unit] { (_, error) =>
            if (error != null) cb(ZIO.fail(error))
            else cb(ZIO.unit)
          }
      }
    }
    baseHandler
      .withDeserializers(CoreDeserializer(keyDeserializer), CoreDeserializer(valueDeserializer))
      .withErrorHandler {
        // TODO handle errors
        case Left(serializationError) => ZIO.unit
        case Right(userError) => ZIO.unit
      }
  }

}
