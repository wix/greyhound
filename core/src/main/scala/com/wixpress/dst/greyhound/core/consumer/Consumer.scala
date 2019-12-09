package com.wixpress.dst.greyhound.core.consumer

import java.util.Properties

import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.{Offset, TopicName}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata, ConsumerConfig => KafkaConsumerConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import zio.blocking.{Blocking, effectBlocking}
import zio.duration.Duration
import zio.{RIO, Semaphore, ZManaged}

import scala.collection.JavaConverters._

trait Consumer {
  def subscribe(topics: Set[TopicName]): RIO[Blocking, Unit]

  def poll(timeout: Duration): RIO[Blocking, Records]

  def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking, Unit]
}

object Consumer {
  type Key = Array[Byte]
  type Value = Array[Byte]
  type Record = ConsumerRecord[Key, Value]
  type Records = ConsumerRecords[Key, Value]

  private val deserializer = new ByteArrayDeserializer

  def make(config: ConsumerConfig): ZManaged[Blocking, Throwable, Consumer] =
    (makeConsumer(config) zipWith Semaphore.make(1).toManaged_) { (consumer, semaphore) =>
      new Consumer {
        override def subscribe(topics: Set[TopicName]): RIO[Blocking, Unit] =
          withConsumer(_.subscribe(topics.asJava))

        override def poll(timeout: Duration): RIO[Blocking, Records] =
          withConsumer(_.poll(timeout.toMillis))

        override def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking, Unit] =
          withConsumer(_.commitSync(offsets.mapValues(new OffsetAndMetadata(_)).asJava))

        private def withConsumer[A](f: KafkaConsumer[Key, Value] => A): RIO[Blocking, A] =
          semaphore.withPermit(effectBlocking(f(consumer)))
      }
    }

  private def makeConsumer(config: ConsumerConfig): ZManaged[Blocking, Throwable, KafkaConsumer[Key, Value]] = {
    val acquire = effectBlocking(new KafkaConsumer(config.properties, deserializer, deserializer))
    ZManaged.make(acquire)(consumer => effectBlocking(consumer.close()).ignore)
  }

}

case class ConsumerConfig(bootstrapServers: Set[String],
                          groupId: String,
                          clientId: String) {

  def properties: Properties = {
    val props = new Properties
    props.setProperty(KafkaConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.mkString(","))
    props.setProperty(KafkaConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.setProperty(KafkaConsumerConfig.CLIENT_ID_CONFIG, clientId)
    props.setProperty(KafkaConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.setProperty(KafkaConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props
  }

}
