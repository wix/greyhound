package com.wixpress.dst.greyhound.core.producer

import java.util.Properties

import com.wixpress.dst.greyhound.core._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig => KafkaProducerConfig, ProducerRecord => KafkaProducerRecord, RecordMetadata => KafkaRecordMetadata}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import zio._
import zio.blocking.{Blocking, effectBlocking}

import scala.collection.JavaConverters._

trait Producer {
  def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): IO[ProducerError, RecordMetadata]

  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): IO[ProducerError, RecordMetadata] = {
    val serializedRecord = for {
      keyBytes <- ZIO.foreach(record.key)(keySerializer.serialize(record.topic, _))
      valueBytes <- valueSerializer.serialize(record.topic, record.value)
    } yield ProducerRecord(
      topic = record.topic,
      value = valueBytes,
      key = keyBytes.headOption,
      partition = record.partition,
      headers = record.headers)

    serializedRecord
      .mapError(SerializationError)
      .flatMap(produce)
  }
}

object Producer {
  private val serializer = new ByteArraySerializer

  def make(config: ProducerConfig): RManaged[Blocking, Producer] = {
    val acquire = effectBlocking(new KafkaProducer(config.properties, serializer, serializer))
    ZManaged.make(acquire)(producer => effectBlocking(producer.close()).ignore).map { producer =>
      new Producer {
        override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): IO[ProducerError, RecordMetadata] =
          ZIO.effectAsync[Any, ProducerError, RecordMetadata] { cb =>
            producer.send(recordFrom(record), new Callback {
              override def onCompletion(metadata: KafkaRecordMetadata, exception: Exception): Unit =
                if (exception != null) cb(ProducerError(exception))
                else cb(ZIO.succeed(RecordMetadata(metadata)))
            })
          }

        private def recordFrom(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) =
          new KafkaProducerRecord(
            record.topic,
            record.partition.fold[Integer](null)(Integer.valueOf),
            record.key.fold[Array[Byte]](null)(_.toArray),
            record.value.toArray,
            headersFrom(record.headers).asJava)

        private def headersFrom(headers: Headers): Iterable[Header] =
          headers.headers.map {
            case (key, value) =>
              new RecordHeader(key, value.toArray)
          }
      }
    }
  }
}

// TODO rename consumer's `Record`?
case class ProducerRecord[+K, +V](topic: TopicName,
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
