package com.wixpress.dst.greyhound.core.producer

import java.util.Properties

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.serialization.Serializer
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata, ProducerConfig => KafkaProducerConfig}
import org.apache.kafka.common.serialization.ByteArraySerializer
import zio.blocking.{Blocking, effectBlocking}
import zio.{Task, ZIO, ZManaged}

trait Producer[K, V] {
  def produce(topic: Topic[K, V], value: V, target: ProduceTarget[K] = ProduceTarget.None): Task[RecordMetadata]

  def produce(topic: Topic[K, V], key: K, value: V): Task[RecordMetadata] =
    produce(topic, value, ProduceTarget.Key(key))
}

sealed trait ProduceTarget[+K]

object ProduceTarget {
  case object None extends ProduceTarget[Nothing]
  case class Partition(partition: Int) extends ProduceTarget[Nothing]
  case class Key[K](key: K) extends ProduceTarget[K]
}

object Producer {
  type Key = Array[Byte]
  type Value = Array[Byte]
  type Record = ProducerRecord[Key, Value]

  private val serializer = new ByteArraySerializer

  def make[K, V](config: ProducerConfig,
                 keySerializer: Serializer[K],
                 valueSerializer: Serializer[V]): ZManaged[Blocking, Throwable, Producer[K, V]] = {
    val acquire = effectBlocking(new KafkaProducer(config.properties, serializer, serializer))
    ZManaged.make(acquire)(producer => effectBlocking(producer.close()).ignore).map { producer =>
      new Producer[K, V] {
        override def produce(topic: Topic[K, V], value: V, target: ProduceTarget[K]): Task[RecordMetadata] =
          recordFrom(topic, value, target).flatMap { record =>
            ZIO.effectAsync[Any, Throwable, RecordMetadata] { cb =>
              producer.send(record, new Callback {
                override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
                  if (exception != null) cb(ZIO.fail(exception))
                  else cb(ZIO.succeed(metadata))
              })
            }
          }

        private def recordFrom(topic: Topic[K, V], value: V, target: ProduceTarget[K]): Task[Record] =
          target match {
            case ProduceTarget.None =>
              valueSerializer.serialize(topic.name, value).map { valueBytes =>
                new ProducerRecord(topic.name, valueBytes)
              }

            case ProduceTarget.Partition(partition) =>
              valueSerializer.serialize(topic.name, value).map { valueBytes =>
                new ProducerRecord(topic.name, partition, null, valueBytes)
              }

            case ProduceTarget.Key(key) => for {
              keyBytes <- keySerializer.serialize(topic.name, key)
              valueBytes <- valueSerializer.serialize(topic.name, value)
            } yield new ProducerRecord(topic.name, keyBytes, valueBytes)
          }
      }
    }
  }
}

case class ProducerConfig(bootstrapServers: Set[String]) {

  def properties: Properties = {
    val props = new Properties
    props.setProperty(KafkaProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.mkString(","))
    props
  }

}
