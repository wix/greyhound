package com.wixpress.dst.greyhound.core.producer

import java.util.Properties

import com.wixpress.dst.greyhound.core._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig => KafkaProducerConfig, ProducerRecord => KafkaProducerRecord, RecordMetadata => KafkaRecordMetadata}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import zio.blocking.{Blocking, effectBlocking}
import zio.{IO, ZIO, ZManaged}

import scala.collection.JavaConverters._

trait Producer {
  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): IO[ProducerError, RecordMetadata]
}

object Producer {
  type Record = KafkaProducerRecord[Array[Byte], Array[Byte]]

  private val serializer = new ByteArraySerializer

  def make(config: ProducerConfig): ZManaged[Blocking, Throwable, Producer] = {
    val acquire = effectBlocking(new KafkaProducer(config.properties, serializer, serializer))
    ZManaged.make(acquire)(producer => effectBlocking(producer.close()).ignore).map { producer =>
      new Producer {
        override def produce[K, V](record: ProducerRecord[K, V],
                                   keySerializer: Serializer[K],
                                   valueSerializer: Serializer[V]): IO[ProducerError, RecordMetadata] = {
          recordFrom(record, keySerializer, valueSerializer).flatMap { record =>
            ZIO.effectAsync[Any, ProducerError, RecordMetadata] { cb =>
              producer.send(record, new Callback {
                override def onCompletion(metadata: KafkaRecordMetadata, exception: Exception): Unit =
                  if (exception != null) cb(ProducerError(exception))
                  else cb(ZIO.succeed(RecordMetadata(metadata)))
              })
            }
          }
        }

        private def recordFrom[K, V](record: ProducerRecord[K, V],
                                     keySerializer: Serializer[K],
                                     valueSerializer: Serializer[V]): IO[ProducerError, Record] =
          (for {
            keyBytes <- ZIO.foreach(record.key)(keySerializer.serialize(record.topic.name, _))
            valueBytes <- valueSerializer.serialize(record.topic.name, record.value)
          } yield new KafkaProducerRecord(
            record.topic.name,
            record.partition.fold[Integer](null)(Integer.valueOf),
            keyBytes.headOption.orNull,
            valueBytes,
            headersFrom(record.headers).asJava)).mapError(SerializationError)

        private def headersFrom(headers: Headers): Iterable[Header] =
          headers.headers.map {
            case (key, value) =>
              new RecordHeader(key, value)
          }
      }
    }
  }
}

// TODO rename consumer's `Record`?
case class ProducerRecord[+K, +V](topic: Topic[K, V],
                                  value: V,
                                  key: Option[K] = None,
                                  partition: Option[Partition] = None,
                                  headers: Headers = Headers.Empty)

case class ProducerConfig(bootstrapServers: Set[String]) {

  def properties: Properties = {
    val props = new Properties
    props.setProperty(KafkaProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.mkString(","))
    props
  }

}
