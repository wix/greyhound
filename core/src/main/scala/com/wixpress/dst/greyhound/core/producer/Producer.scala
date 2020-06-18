package com.wixpress.dst.greyhound.core.producer

import java.util.Properties

import com.wixpress.dst.greyhound.core._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig => KafkaProducerConfig, ProducerRecord => KafkaProducerRecord, RecordMetadata => KafkaRecordMetadata}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.duration._

import scala.collection.JavaConverters._

trait Producer[-R] {
  def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R, ProducerError, RecordMetadata]

  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): ZIO[R, ProducerError, RecordMetadata] = {
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

  def make[R](config: ProducerConfig): RManaged[Blocking, Producer[R]] = {
    val acquire = effectBlocking(new KafkaProducer(config.properties, serializer, serializer))
    ZManaged.make(acquire)(producer => effectBlocking(producer.close()).ignore).map { producer =>
      new Producer[R] {
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

case class ProducerConfig(bootstrapServers: String,
                          retryPolicy: ProducerRetryPolicy = ProducerRetryPolicy.Default,
                          extraProperties: Map[String, String] = Map.empty) {
  def withBootstrapServers(servers: String) = copy(bootstrapServers = servers)

  def withRetryPolicy(retryPolicy: ProducerRetryPolicy) = copy(retryPolicy = retryPolicy)

  def withProperties(extraProperties: Map[String, String]) = copy(extraProperties = extraProperties)

  def properties: Properties = {
    val props = new Properties
    props.setProperty(KafkaProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.setProperty(KafkaProducerConfig.RETRIES_CONFIG, retryPolicy.retries.toString)
    props.setProperty(KafkaProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryPolicy.backoff.toMillis.toString)
    props.setProperty(KafkaProducerConfig.ACKS_CONFIG, "all")
    extraProperties.foreach {
      case (key, value) =>
        props.setProperty(key, value)
    }
    props
  }

}

case class ProducerRetryPolicy(retries: Int, backoff: Duration)

object ProducerRetryPolicy {
  val Default = ProducerRetryPolicy(30, 200.millis)
}
