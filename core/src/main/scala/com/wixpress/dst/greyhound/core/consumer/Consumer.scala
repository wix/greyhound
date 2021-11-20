package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern
import java.{lang, time, util}
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.Consumer._
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.ClosedConsumer
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, Decryptor, NoOpDecryptor, RecordTopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, ConsumerConfig => KafkaConsumerConfig}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.clock.Clock
import zio.duration._

import scala.collection.JavaConverters._
import scala.util.{Random, Try}

trait Consumer {
  def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1] = RebalanceListener.Empty): RIO[Blocking with GreyhoundMetrics with R1, Unit]

  def subscribePattern[R1](topicStartsWith: Pattern, rebalanceListener: RebalanceListener[R1] = RebalanceListener.Empty): RIO[Blocking with GreyhoundMetrics with R1, Unit]

  def poll(timeout: Duration): RIO[Blocking with GreyhoundMetrics, Records]

  def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking with GreyhoundMetrics, Unit]

  def endOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]]

  def committedOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]]

  def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Clock with Blocking, Map[TopicPartition, Offset]]

  def commitOnRebalance(offsets: Map[TopicPartition, Offset]): RIO[Blocking with GreyhoundMetrics, DelayedRebalanceEffect]

  def pause(partitions: Set[TopicPartition]): ZIO[Blocking with GreyhoundMetrics, IllegalStateException, Unit]

  def resume(partitions: Set[TopicPartition]): ZIO[Blocking with GreyhoundMetrics, IllegalStateException, Unit]

  def seek(partition: TopicPartition, offset: Offset): ZIO[Blocking with GreyhoundMetrics, IllegalStateException, Unit]

  def seek(toOffsets: Map[TopicPartition, Offset]): ZIO[Blocking with GreyhoundMetrics, Nothing, Unit] =
    ZIO.foreach(toOffsets.toSeq){case (tp, o) => seek(tp, o).ignore}.unit

  def pause(record: ConsumerRecord[_, _]): ZIO[Blocking with GreyhoundMetrics, IllegalStateException, Unit] = {
    val partition = RecordTopicPartition(record)
    pause(Set(partition)) *> seek(partition, record.offset)
  }

  def position(topicPartition: TopicPartition): Task[Offset]

  def assignment: Task[Set[TopicPartition]]

  def config: ConsumerConfig

  def listTopics: RIO[Blocking, Map[Topic, List[PartitionInfo]]]
}

object Consumer {
  type Record = ConsumerRecord[Chunk[Byte], Chunk[Byte]]
  type Records = Iterable[Record]

  private val deserializer = new Deserializer[Chunk[Byte]] {
    override def configure(configs: util.Map[Topic, _], isKey: Boolean): Unit = ()

    override def deserialize(topic: Topic, data: Array[Byte]): Chunk[Byte] = Chunk.fromArray(data)

    override def close(): Unit = ()
  }

  def make(cfg: ConsumerConfig): RManaged[Blocking with GreyhoundMetrics, Consumer] = for {
    semaphore <- Semaphore.make(1).toManaged_
    consumer <- makeConsumer(cfg, semaphore)
    metrics <- ZIO.environment[GreyhoundMetrics].toManaged_
    // we commit missing offsets to current position on assign - otherwise messages may be lost, in case of `OffsetReset.Latest`,
    // if a partition with no committed offset is revoked during processing
    // we also may want to seek forward to some given initial offsets
    offsetsInitializer <- OffsetsInitializer.make(
      cfg.clientId,
      cfg.groupId,
      UnsafeOffsetOperations.make(consumer),
      timeout = 500.millis,
      timeoutIfSeek = 10.seconds,
      initialSeek = cfg.initialSeek).toManaged_
  } yield {
    new Consumer {
      override def subscribePattern[R1](pattern: Pattern, rebalanceListener: RebalanceListener[R1]): RIO[Blocking with R1, Unit] =
        listener(this, offsetsInitializer.initializeOffsets, config.additionalListener *> rebalanceListener)
          .flatMap(lis => withConsumer(_.subscribe(pattern, lis)))

      override def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1]): RIO[Blocking with R1, Unit] =
        listener(this, offsetsInitializer.initializeOffsets, config.additionalListener *> rebalanceListener)
          .flatMap(lis => withConsumerBlocking(_.subscribe(topics.asJava, lis)))

      override def poll(timeout: Duration): RIO[Blocking, Records] =
        withConsumerM { c =>
          rewindPositionsOnError(c) {
            effectBlocking(c.poll(time.Duration.ofMillis(timeout.toMillis)).asScala.map(ConsumerRecord(_)))
              .flatMap(ZIO.foreach(_)(cfg.decryptor.decrypt))
          }
        }

      private def rewindPositionsOnError[R, A](c: KafkaConsumer[Chunk[Byte], Chunk[Byte]])(op: ZIO[R, Throwable, A]) = {
        def rewind(positions: Iterable[(TopicPartition, Offset)]) =
          seekUnsafe(positions)(c)
          .resurrect
          .reporting(ConsumerMetric.RewindOffsetsOnPollError(cfg.clientId, cfg.groupId, positions.toMap, _))
          .provideSome[Blocking](_ ++ metrics)
          .ignore
        for {
          positions <- allPositionsUnsafe
          result <- op.tapCause(_ => rewind(positions))
        } yield result
      }

      override def endOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]] =
        withConsumerBlocking(_.endOffsets(kafkaPartitions(partitions)))
        .map(_.asScala.map { case (tp: KafkaTopicPartition, o: lang.Long) => (TopicPartition(tp), o.toLong)}.toMap)


      override def beginningOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]] =
        withConsumerBlocking(_.beginningOffsets(kafkaPartitions(partitions)))
          .map(_.asScala.map { case (tp: KafkaTopicPartition, o: lang.Long) => (TopicPartition(tp), o.toLong)}.toMap)

      override def committedOffsets(partitions: Set[TopicPartition]): RIO[Blocking, Map[TopicPartition, Offset]] =
        withConsumerBlocking(_.committed(kafkaPartitions(partitions)))
          .map(_.asScala.collect { case (tp: KafkaTopicPartition, o: OffsetAndMetadata) => (TopicPartition(tp), o.offset)}.toMap)

      override def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking with GreyhoundMetrics, Unit] = {
        withConsumerBlocking(_.commitSync(kafkaOffsets(offsets)))
      }

      override def commitOnRebalance(offsets: Map[TopicPartition, Offset]): RIO[Blocking with GreyhoundMetrics, DelayedRebalanceEffect] = {
        val kOffsets = kafkaOffsets(offsets)
        // we can't actually call commit here, as it needs to be called from the same
        // thread, that triggered poll(), so we return the commit action as thunk
        UIO(DelayedRebalanceEffect(consumer.commitSync(kOffsets)))
      }

      override def pause(partitions: Set[TopicPartition]): ZIO[Any, IllegalStateException, Unit] =
        withConsumer(_.pause(kafkaPartitions(partitions))).refineOrDie {
          case e: IllegalStateException => e
        }

      override def resume(partitions: Set[TopicPartition]): ZIO[Any, IllegalStateException, Unit] =
        withConsumer(consumer => {
          val onlySubscribed = consumer.assignment().asScala.toSet intersect kafkaPartitions(partitions).asScala.toSet
          consumer.resume(onlySubscribed.asJavaCollection)
        }).refineOrDie {
          case e: IllegalStateException => e
        }

      override def seek(partition: TopicPartition, offset: Offset): ZIO[Any, IllegalStateException, Unit] =
        withConsumerM(seekUnsafe(Seq(partition -> offset)))

      private def seekUnsafe(positions: Iterable[(TopicPartition, Offset)])(c: KafkaConsumer[Chunk[Byte], Chunk[Byte]]) = {
        Task(positions.foreach {
          case (partition, offset) => c.seek(partition.asKafka, offset)
        }).refineOrDie {
          case e: IllegalStateException => e
        }
      }

      override def position(topicPartition: TopicPartition): Task[Offset] =
        withConsumer(_.position(topicPartition.asKafka))

      override def assignment: Task[Set[TopicPartition]] = {
        withConsumer(_.assignment().asScala.toSet.map(TopicPartition.apply(_: org.apache.kafka.common.TopicPartition)))
      }

      private def allPositionsUnsafe = effectBlocking {
        consumer.assignment().asScala.toSet
          .map((tp: KafkaTopicPartition) => TopicPartition(tp) -> consumer.position(tp))
          .toMap
      }

      override def config: ConsumerConfig = cfg

      private def withConsumer[A](f: KafkaConsumer[Chunk[Byte], Chunk[Byte]] => A): Task[A] =
        semaphore.withPermit(Task(f(consumer)))

      private def withConsumerBlocking[A](f: KafkaConsumer[Chunk[Byte], Chunk[Byte]] => A): RIO[Blocking, A] =
        semaphore.withPermit(effectBlocking(f(consumer)))

      private def withConsumerM[R, A, E](f: KafkaConsumer[Chunk[Byte], Chunk[Byte]] => ZIO[R, E, A]): ZIO[R, E, A] =
        semaphore.withPermit(f(consumer))

      override def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Clock with Blocking, Map[TopicPartition, Offset]] = {
        val kafkaTopicPartitionsOnTimestamp = topicPartitionsOnTimestamp.map { case (tp, ts) => tp.asKafka -> ts }
        withConsumerBlocking(_.offsetsForTimes(kafkaTopicPartitionsOnTimestamp.mapValues(l => new lang.Long(l)).asJava))
          .map(_.asScala.filter { case (_, offset) => offset != null }
            .map { case (ktp, offset) => TopicPartition(ktp) -> offset.offset()}.toMap)
      }

      override def listTopics: RIO[Blocking, Map[Topic, List[PartitionInfo]]] =
        withConsumer(_.listTopics()).map { topics =>
          topics.asScala.mapValues(_.asScala.toList.map(PartitionInfo.apply)).toMap
        }
    }
  }

  private def listener[R1](consumer: Consumer, onAssignFirstDo: Set[TopicPartition] => Unit, rebalanceListener: RebalanceListener[R1]) =
    ZIO.runtime[Blocking with R1].map { runtime =>
      new ConsumerRebalanceListener {
        override def onPartitionsRevoked(partitions: util.Collection[KafkaTopicPartition]): Unit = {
          runtime.unsafeRun(rebalanceListener.onPartitionsRevoked(consumer, partitionsFor(partitions)))
            .run() // this needs to be run in the same thread
        }

        override def onPartitionsAssigned(partitions: util.Collection[KafkaTopicPartition]): Unit = {
          val assigned = partitionsFor(partitions)
          onAssignFirstDo(assigned)
          runtime.unsafeRun(rebalanceListener.onPartitionsAssigned(consumer, assigned))
        }

        private def partitionsFor(partitions: util.Collection[KafkaTopicPartition]) =
          partitions.asScala.map(TopicPartition(_)).toSet
      }
    }

  private def makeConsumer(config: ConsumerConfig, semaphore: Semaphore): RManaged[Blocking with GreyhoundMetrics, KafkaConsumer[Chunk[Byte], Chunk[Byte]]] = {
    val acquire = effectBlocking(new KafkaConsumer(config.properties, deserializer, deserializer))
    def close(consumer: KafkaConsumer[_, _]) =
      effectBlocking(consumer.close())
      .reporting(ClosedConsumer(config.groupId, config.clientId, _))
      .ignore

    ZManaged.make(acquire)(consumer =>
      semaphore.withPermit(close(consumer))
    )
  }


}

case class ConsumerConfig(bootstrapServers: String,
                          groupId: Group,
                          clientId: ClientId = s"wix-consumer-${Random.alphanumeric.take(5).mkString}",
                          offsetReset: OffsetReset = OffsetReset.Latest,
                          extraProperties: Map[String, String] = Map.empty,
                          additionalListener: RebalanceListener[Any] = RebalanceListener.Empty,
                          initialSeek: InitialOffsetsSeek =InitialOffsetsSeek.default,
                          consumerAttributes: Map[String, String] = Map.empty,
                          decryptor: Decryptor[Any, Throwable, Chunk[Byte], Chunk[Byte]] = new NoOpDecryptor
                         ) extends CommonGreyhoundConfig {


  override def kafkaProps: Map[String, String] = Map(
    KafkaConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    KafkaConsumerConfig.GROUP_ID_CONFIG -> groupId,
    KafkaConsumerConfig.CLIENT_ID_CONFIG -> clientId,
    (KafkaConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset match {
      case OffsetReset.Earliest => "earliest"
      case OffsetReset.Latest => "latest"
    }),
    KafkaConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ) ++ extraProperties

  def withExtraProperties(props: (String, String)*) =
    copy(extraProperties = extraProperties ++ props)

}

sealed trait OffsetReset

object OffsetReset {

  case object Earliest extends OffsetReset

  case object Latest extends OffsetReset

}

trait UnsafeOffsetOperations {
  def committed(partitions: Set[TopicPartition], timeout: zio.duration.Duration): Map[TopicPartition, Offset]

  def position(partition: TopicPartition, timeout: zio.duration.Duration): Offset

  def commit(offsets: Map[TopicPartition, Offset], timeout: Duration): Unit

  def seek(offsets: Map[TopicPartition, Offset]): Unit

  def endOffsets(partitions: Set[TopicPartition],
                 timeout: Duration): Map[TopicPartition, Offset]
}

object UnsafeOffsetOperations {
  def make(consumer: KafkaConsumer[_, _]) = new UnsafeOffsetOperations {
    override def committed(partitions: Set[TopicPartition],
                           timeout: Duration): Map[TopicPartition, Offset] = {
      consumer.committed(partitions.map(_.asKafka).asJava, timeout)
        .asScala
        .toMap
        .collect {
          case (tp, ofm) if ofm != null =>
            TopicPartition(tp) -> ofm.offset()
        }
    }

    override def position(partition: TopicPartition,
                          timeout: Duration): Offset =
      consumer.position(partition.asKafka, timeout)

    override def commit(offsets: Map[TopicPartition, Offset], timeout: Duration): Unit = {
      consumer.commitSync(kafkaOffsets(offsets), timeout)
    }

    override def seek(offsets: Map[TopicPartition, Offset]): Unit =
      offsets.foreach {
        case (tp, offset) => Try(consumer.seek(tp.asKafka, offset))
      }

    override def endOffsets(partitions: Set[TopicPartition],
                   timeout: Duration): Map[TopicPartition, Long] = {
      consumer.endOffsets(partitions.map(_.asKafka).asJava, timeout)
        .asScala.toMap.map {
        case (tp, of) => TopicPartition(tp) -> (of: Long)
      }
    }
  }
}
