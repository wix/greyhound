package com.wixpress.dst.greyhound.core.consumer

import java.util
import java.util.Properties

import com.wixpress.dst.greyhound.core.consumer.Consumer._
import com.wixpress.dst.greyhound.core.{Offset, Topic}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer, OffsetAndMetadata, ConsumerConfig => KafkaConsumerConfig, ConsumerRebalanceListener => KafkaConsumerRebalanceListener, ConsumerRecord => KafkaConsumerRecord}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.duration.Duration

import scala.collection.JavaConverters._

trait Consumer[R] {
  def subscribe(topics: Set[Topic],
                onPartitionsRevoked: RebalanceListener[R] = _ => ZIO.unit,
                onPartitionsAssigned: RebalanceListener[R] = _ => ZIO.unit): RIO[R, Unit]

  def poll(timeout: Duration): RIO[R, Records]

  def commit(offsets: Map[TopicPartition, Offset]): RIO[R, Unit]

  def pause(partitions: Set[TopicPartition]): RIO[R, Unit]

  def resume(partitions: Set[TopicPartition]): RIO[R, Unit]

  def seek(partition: TopicPartition, offset: Offset): RIO[R, Unit]

  def partitionsFor(topic: Topic): RIO[R, Set[TopicPartition]]

  def partitionsFor(topics: Set[Topic]): RIO[R, Set[TopicPartition]] =
    ZIO.foldLeft(topics)(Set.empty[TopicPartition]) { (acc, topic) =>
      partitionsFor(topic).map(acc union _)
    }
}

object Consumer {
  type Record = KafkaConsumerRecord[Chunk[Byte], Chunk[Byte]]
  type Records = ConsumerRecords[Chunk[Byte], Chunk[Byte]]
  type RebalanceListener[R] = Set[TopicPartition] => URIO[R, Unit]

  private val deserializer = new Deserializer[Chunk[Byte]] {
    override def configure(configs: util.Map[Topic, _], isKey: Boolean): Unit = ()
    override def deserialize(topic: Topic, data: Array[Byte]): Chunk[Byte] = Chunk.fromArray(data)
    override def close(): Unit = ()
  }

  def make[R](config: ConsumerConfig): RManaged[R with Blocking, Consumer[R with Blocking]] =
    (makeConsumer(config) zipWith Semaphore.make(1).toManaged_) { (consumer, semaphore) =>
      new Consumer[R with Blocking] {
        override def subscribe(topics: Set[Topic],
                               onPartitionsRevoked: RebalanceListener[R with Blocking],
                               onPartitionsAssigned: RebalanceListener[R with Blocking]): RIO[R with Blocking, Unit] = {
          val onPartitionsRevoked1 = onPartitionsRevoked
          val onPartitionsAssigned1 = onPartitionsAssigned
          for {
            runtime <- ZIO.runtime[R with Blocking]
            rebalanceListener = new KafkaConsumerRebalanceListener {
              override def onPartitionsRevoked(partitions: util.Collection[KafkaTopicPartition]): Unit =
                runtime.unsafeRun(onPartitionsRevoked1(partitionsFor(partitions)))

              override def onPartitionsAssigned(partitions: util.Collection[KafkaTopicPartition]): Unit =
                runtime.unsafeRun(onPartitionsAssigned1(partitionsFor(partitions)))

              private def partitionsFor(partitions: util.Collection[KafkaTopicPartition]) =
                partitions.asScala.foldLeft(Set.empty[TopicPartition])(_ + TopicPartition(_))
            }
            _ <- withConsumer(_.subscribe(topics.asJava, rebalanceListener))
          } yield ()
        }

        override def poll(timeout: Duration): RIO[Blocking, Records] =
          withConsumer(_.poll(timeout.toMillis))

        override def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking, Unit] =
          withConsumer(_.commitSync(kafkaOffsets(offsets)))

        override def pause(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
          withConsumer(_.pause(kafkaPartitions(partitions)))

        override def resume(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
          withConsumer(_.resume(kafkaPartitions(partitions)))

        override def seek(partition: TopicPartition, offset: Offset): RIO[Blocking, Unit] =
          withConsumer(_.seek(new KafkaTopicPartition(partition.topic, partition.partition), offset))

        override def partitionsFor(topic: Topic): RIO[Blocking, Set[TopicPartition]] =
          withConsumer(_.partitionsFor(topic)).map { partitions =>
            val safePartitions = Option(partitions).map(_.asScala).getOrElse(Nil)
            safePartitions.foldLeft(Set.empty[TopicPartition]) { (acc, partition) =>
              acc + TopicPartition(topic, partition.partition)
            }
          }

        private def withConsumer[A](f: KafkaConsumer[Chunk[Byte], Chunk[Byte]] => A): RIO[Blocking, A] =
          semaphore.withPermit(effectBlocking(f(consumer)))

        private def kafkaOffsets(offsets: Map[TopicPartition, Offset]): util.Map[KafkaTopicPartition, OffsetAndMetadata] =
          offsets.foldLeft(new util.HashMap[KafkaTopicPartition, OffsetAndMetadata](offsets.size)) {
            case (acc, (TopicPartition(topic, partition), offset)) =>
              val key = new KafkaTopicPartition(topic, partition)
              val value = new OffsetAndMetadata(offset)
              acc.put(key, value)
              acc
          }

        private def kafkaPartitions(partitions: Set[TopicPartition]): util.Collection[KafkaTopicPartition] =
          partitions.foldLeft(new util.ArrayList[KafkaTopicPartition](partitions.size)) {
            case (acc, TopicPartition(topic, partition)) =>
              acc.add(new KafkaTopicPartition(topic, partition))
              acc
          }
      }
    }

  private def makeConsumer(config: ConsumerConfig): RManaged[Blocking, KafkaConsumer[Chunk[Byte], Chunk[Byte]]] = {
    val acquire = effectBlocking(new KafkaConsumer(config.properties, deserializer, deserializer))
    ZManaged.make(acquire)(consumer => effectBlocking(consumer.close()).ignore)
  }

}

case class ConsumerConfig(bootstrapServers: Set[String],
                          groupId: String,
                          clientId: String,
                          offsetReset: OffsetReset = OffsetReset.Latest,
                          extraProperties: Map[String, String] = Map.empty) {

  def properties: Properties = {
    val props = new Properties
    props.setProperty(KafkaConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.mkString(","))
    props.setProperty(KafkaConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.setProperty(KafkaConsumerConfig.CLIENT_ID_CONFIG, clientId)
    props.setProperty(KafkaConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset match {
      case OffsetReset.Earliest => "earliest"
      case OffsetReset.Latest => "latest"
    })
    props.setProperty(KafkaConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    extraProperties.foreach {
      case (key, value) =>
        props.setProperty(key, value)
    }
    props
  }

}

sealed trait OffsetReset
object OffsetReset {
  case object Earliest extends OffsetReset
  case object Latest extends OffsetReset
}
