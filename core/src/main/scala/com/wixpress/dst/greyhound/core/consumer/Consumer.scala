package com.wixpress.dst.greyhound.core.consumer

import java.util
import java.util.Properties

import com.wixpress.dst.greyhound.core.consumer.Consumer._
import com.wixpress.dst.greyhound.core.{ClientId, Group, Offset, Topic}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer, OffsetAndMetadata, ConsumerConfig => KafkaConsumerConfig, ConsumerRecord => KafkaConsumerRecord}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.duration.Duration

import scala.collection.JavaConverters._

trait Consumer[R] {
  def subscribe(topics: Set[Topic], rebalanceListener: RebalanceListener[R] = RebalanceListener.Empty): RIO[R, Unit]

  def poll(timeout: Duration): RIO[R, Records]

  def commit(offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean = false): RIO[R, Unit]

  def pause(partitions: Set[TopicPartition]): ZIO[R, IllegalStateException, Unit]

  def resume(partitions: Set[TopicPartition]): ZIO[R, IllegalStateException, Unit]

  def seek(partition: TopicPartition, offset: Offset): ZIO[R, IllegalStateException, Unit]

  def pause(record: ConsumerRecord[_, _]): ZIO[R, IllegalStateException, Unit] = {
    val partition = TopicPartition(record)
    pause(Set(partition)) *> seek(partition, record.offset)
  }
}

object Consumer {
  type Record = KafkaConsumerRecord[Chunk[Byte], Chunk[Byte]]
  type Records = ConsumerRecords[Chunk[Byte], Chunk[Byte]]

  private val deserializer = new Deserializer[Chunk[Byte]] {
    override def configure(configs: util.Map[Topic, _], isKey: Boolean): Unit = ()
    override def deserialize(topic: Topic, data: Array[Byte]): Chunk[Byte] = Chunk.fromArray(data)
    override def close(): Unit = ()
  }

  def make(config: ConsumerConfig): RManaged[Blocking, Consumer[Blocking]] = for {
    semaphore <- Semaphore.make(1).toManaged_
    consumer <- makeConsumer(config)
  } yield new Consumer[Blocking] {
    override def subscribe(topics: Set[Topic], rebalanceListener: RebalanceListener[Blocking]): RIO[Blocking, Unit] =
      for {
        runtime <- ZIO.runtime[Blocking]
        consumerRebalanceListener = new ConsumerRebalanceListener {
          override def onPartitionsRevoked(partitions: util.Collection[KafkaTopicPartition]): Unit =
            runtime.unsafeRun(rebalanceListener.onPartitionsRevoked(partitionsFor(partitions)))

          override def onPartitionsAssigned(partitions: util.Collection[KafkaTopicPartition]): Unit =
            runtime.unsafeRun(rebalanceListener.onPartitionsAssigned(partitionsFor(partitions)))

          private def partitionsFor(partitions: util.Collection[KafkaTopicPartition]) =
            partitions.asScala.map(TopicPartition(_)).toSet
        }
        _ <- withConsumer(_.subscribe(topics.asJava, consumerRebalanceListener))
      } yield ()

    override def poll(timeout: Duration): RIO[Blocking, Records] =
      withConsumer(_.poll(timeout.toMillis))

    override def commit(offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean): RIO[Blocking, Unit] =
      if (calledOnRebalance) Task(consumer.commitSync(kafkaOffsets(offsets)))
      else withConsumer(_.commitSync(kafkaOffsets(offsets)))

    override def pause(partitions: Set[TopicPartition]): ZIO[Blocking, IllegalStateException, Unit] =
      withConsumer(_.pause(kafkaPartitions(partitions))).refineOrDie {
        case e: IllegalStateException => e
      }

    override def resume(partitions: Set[TopicPartition]): ZIO[Blocking, IllegalStateException, Unit] =
      withConsumer(_.resume(kafkaPartitions(partitions))).refineOrDie {
        case e: IllegalStateException => e
      }

    override def seek(partition: TopicPartition, offset: Offset): ZIO[Blocking, IllegalStateException, Unit] =
      withConsumer(_.seek(new KafkaTopicPartition(partition.topic, partition.partition), offset)).refineOrDie {
        case e: IllegalStateException => e
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

  private def makeConsumer(config: ConsumerConfig): RManaged[Blocking, KafkaConsumer[Chunk[Byte], Chunk[Byte]]] = {
    val acquire = effectBlocking(new KafkaConsumer(config.properties, deserializer, deserializer))
    ZManaged.make(acquire)(consumer => effectBlocking(consumer.close()).ignore)
  }

}

case class ConsumerConfig(bootstrapServers: Set[String],
                          groupId: Group,
                          clientId: ClientId,
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
