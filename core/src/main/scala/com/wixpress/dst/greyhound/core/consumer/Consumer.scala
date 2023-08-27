package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.Consumer._
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.ClosedConsumer
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, Decryptor, NoOpDecryptor, RecordTopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics._
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, ConsumerConfig => KafkaConsumerConfig, OffsetAndMetadata => KafkaOffsetAndMetadata}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import zio.ZIO.attemptBlocking
import zio._

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.{lang, time, util}
import scala.collection.JavaConverters._
import scala.util.{Random, Try}

trait Consumer {
  def subscribe[R1](
    topics: Set[Topic],
    rebalanceListener: RebalanceListener[R1] = RebalanceListener.Empty
  )(implicit trace: Trace): RIO[GreyhoundMetrics with R1, Unit]

  def subscribePattern[R1](
    topicStartsWith: Pattern,
    rebalanceListener: RebalanceListener[R1] = RebalanceListener.Empty
  )(implicit trace: Trace): RIO[GreyhoundMetrics with R1, Unit]

  def poll(timeout: Duration)(implicit trace: Trace): RIO[GreyhoundMetrics, Records]

  def commit(offsets: Map[TopicPartition, Offset])(implicit trace: Trace): RIO[GreyhoundMetrics, Unit]

  def commitWithMetadata(offsetsAndMetadata: Map[TopicPartition, OffsetAndMetadata])(implicit trace: Trace): RIO[GreyhoundMetrics, Unit]

  def endOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]]

  def committedOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]]

  def committedOffsetsAndMetadata(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, OffsetAndMetadata]]

  def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]]

  def commitOnRebalance(offsets: Map[TopicPartition, Offset])(implicit trace: Trace): RIO[GreyhoundMetrics, DelayedRebalanceEffect]

  def committedOffsetsAndMetadataOnRebalance(partitions: Set[TopicPartition])(implicit trace: Trace): Map[TopicPartition, OffsetAndMetadata]

  def commitWithMetadataOnRebalance(offsets: Map[TopicPartition, OffsetAndMetadata])(
    implicit trace: Trace
  ): RIO[GreyhoundMetrics, DelayedRebalanceEffect]

  def pause(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[GreyhoundMetrics, IllegalStateException, Unit]

  def resume(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[GreyhoundMetrics, IllegalStateException, Unit]

  def seek(partition: TopicPartition, offset: Offset)(implicit trace: Trace): ZIO[GreyhoundMetrics, IllegalStateException, Unit]

  def seek(toOffsets: Map[TopicPartition, Offset])(implicit trace: Trace): ZIO[GreyhoundMetrics, Nothing, Unit] =
    ZIO.foreach(toOffsets.toSeq) { case (tp, o) => seek(tp, o).ignore }.unit

  def pause(record: ConsumerRecord[_, _])(implicit trace: Trace): ZIO[GreyhoundMetrics, IllegalStateException, Unit] = {
    val partition = RecordTopicPartition(record)
    pause(Set(partition)) *> seek(partition, record.offset)
  }

  def position(topicPartition: TopicPartition)(implicit trace: Trace): Task[Offset]

  def assignment(implicit trace: Trace): Task[Set[TopicPartition]]

  def assign(tps: Set[TopicPartition])(implicit trace: Trace): Task[Unit] = ZIO.fail(new IllegalStateException("Not implemented"))

  def config(implicit trace: Trace): ConsumerConfig

  def listTopics(implicit trace: Trace): RIO[Any, Map[Topic, List[PartitionInfo]]]

  def shutdown(timeout: Duration)(implicit trace: Trace): Task[Unit] = ZIO.succeed(())
}

object Consumer {
  type Record  = ConsumerRecord[Chunk[Byte], Chunk[Byte]]
  type Records = Iterable[Record]

  private val deserializer = new Deserializer[Chunk[Byte]] {
    override def configure(configs: util.Map[Topic, _], isKey: Boolean): Unit = ()

    override def deserialize(topic: Topic, data: Array[Byte]): Chunk[Byte] = Chunk.fromArray(data)

    override def close(): Unit = ()
  }

  def make(cfg: ConsumerConfig)(implicit trace: Trace): RIO[GreyhoundMetrics with Scope, Consumer] = for {
    semaphore          <- Semaphore.make(1)
    consumer           <- makeConsumer(cfg, semaphore)
    metrics            <- ZIO.environment[GreyhoundMetrics]
    // we commit missing offsets to current position on assign - otherwise messages may be lost, in case of `OffsetReset.Latest`,
    // if a partition with no committed offset is revoked during processing
    // we also may want to seek forward to some given initial offsets
    unsafeOffsetOperations = UnsafeOffsetOperations.make(consumer)
    offsetsInitializer <- OffsetsInitializer
                            .make(
                              cfg.clientId,
                              cfg.groupId,
                              unsafeOffsetOperations,
                              timeout = 10.seconds,
                              timeoutIfSeek = 10.seconds,
                              initialSeek = cfg.initialSeek,
                              rewindUncommittedOffsetsBy = cfg.rewindUncommittedOffsetsByMillis.millis,
                              offsetResetIsEarliest = cfg.offsetResetIsEarliest,
                              parallelConsumer = cfg.useParallelConsumer
                            )
  } yield {
    new Consumer {
      override def subscribePattern[R1](topicStartsWith: Pattern, rebalanceListener: RebalanceListener[R1])(
        implicit trace: Trace
      ): RIO[GreyhoundMetrics with R1, Unit] =
        listener(this, offsetsInitializer.initializeOffsets, config.additionalListener *> rebalanceListener, unsafeOffsetOperations)
          .flatMap(lis => withConsumer(_.subscribe(topicStartsWith, lis)))

      override def subscribe[R1](topics: Set[Topic], rebalanceListener: RebalanceListener[R1])(
        implicit trace: Trace
      ): RIO[GreyhoundMetrics with R1, Unit] =
        listener(this, offsetsInitializer.initializeOffsets, config.additionalListener *> rebalanceListener, unsafeOffsetOperations)
          .flatMap(lis => withConsumerBlocking(_.subscribe(topics.asJava, lis)))

      override def poll(timeout: Duration)(implicit trace: Trace): RIO[Any, Records] =
        withConsumerM { c =>
          rewindPositionsOnError(c) {
            attemptBlocking(c.poll(time.Duration.ofMillis(timeout.toMillis)).asScala.map(rec => ConsumerRecord(rec, config.groupId)))
              .flatMap(ZIO.foreach(_)(cfg.decryptor.decrypt))
          }
        }

      private def rewindPositionsOnError[R, A](c: KafkaConsumer[Chunk[Byte], Chunk[Byte]])(op: ZIO[R, Throwable, A]) = {
        def rewind(positions: Iterable[(TopicPartition, Offset)]) =
          seekUnsafe(positions)(c).resurrect
            .reporting(ConsumerMetric.RewindOffsetsOnPollError(cfg.clientId, cfg.groupId, positions.toMap, _))
            .provideEnvironment(metrics)
            .ignore
        for {
          positions <- allPositionsUnsafe
          result    <- op.tapErrorCause(_ => rewind(positions))
        } yield result
      }

      override def endOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
        withConsumerBlocking(_.endOffsets(kafkaPartitions(partitions)))
          .map(_.asScala.map { case (tp: KafkaTopicPartition, o: lang.Long) => (TopicPartition(tp), o.toLong) }.toMap)

      override def beginningOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
        withConsumerBlocking(_.beginningOffsets(kafkaPartitions(partitions)))
          .map(_.asScala.map { case (tp: KafkaTopicPartition, o: lang.Long) => (TopicPartition(tp), o.toLong) }.toMap)

      override def committedOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] =
        withConsumerBlocking(_.committed(kafkaPartitions(partitions)))
          .map(_.asScala.collect { case (tp: KafkaTopicPartition, o: KafkaOffsetAndMetadata) => (TopicPartition(tp), o.offset) }.toMap)

      override def committedOffsetsAndMetadata(
        partitions: NonEmptySet[TopicPartition]
      )(implicit trace: Trace): RIO[Any, Map[TopicPartition, OffsetAndMetadata]] =
        withConsumerBlocking(_.committed(kafkaPartitions(partitions)))
          .map(
            _.asScala
              .collect {
                case (tp: KafkaTopicPartition, om: KafkaOffsetAndMetadata) =>
                  (TopicPartition(tp), OffsetAndMetadata(om.offset, om.metadata))
              }
              .toMap
          )

      import java.time.format.DateTimeFormatter
      import java.time.LocalDateTime
      private val podName = sys.env.get("POD_NAME")
      private val dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
      def metadata: Option[String] = if (config.enrichMetadata) podName.map(name => s">>> pod: $name, ts: ${dtf.format(LocalDateTime.now())}") else None

      override def commit(offsets: Map[TopicPartition, Offset])(implicit trace: Trace): RIO[GreyhoundMetrics, Unit] = {
        withConsumerBlocking(_.commitSync(kafkaOffsetsAndMetaData(toOffsetsAndMetadata(offsets, metadata.getOrElse(cfg.commitMetadataString)))))
      }

      override def commitWithMetadata(
        offsetsAndMetadata: Map[TopicPartition, OffsetAndMetadata]
      )(implicit trace: Trace): RIO[GreyhoundMetrics, Unit] = {
        withConsumerBlocking(_.commitSync(kafkaOffsetsAndMetaData(offsetsAndMetadata)))
      }

      override def commitOnRebalance(
        offsets: Map[TopicPartition, Offset]
      )(implicit trace: Trace): RIO[GreyhoundMetrics, DelayedRebalanceEffect] = {
        val kOffsets = kafkaOffsetsAndMetaData(toOffsetsAndMetadata(offsets, cfg.commitMetadataString))
        // we can't actually call commit here, as it needs to be called from the same
        // thread, that triggered poll(), so we return the commit action as thunk
        ZIO.succeed(DelayedRebalanceEffect(consumer.commitSync(kOffsets)))
      }

      override def committedOffsetsAndMetadataOnRebalance(partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): Map[TopicPartition, OffsetAndMetadata] = {
        // unsafe function - should only be called from a RebalanceListener
        consumer
          .committed(kafkaPartitions(partitions))
          .asScala
          .collect {
            case (tp: KafkaTopicPartition, om: KafkaOffsetAndMetadata) =>
              (TopicPartition(tp), OffsetAndMetadata(om.offset, om.metadata))
          }
          .toMap
      }

      override def commitWithMetadataOnRebalance(
        offsets: Map[TopicPartition, OffsetAndMetadata]
      )(implicit trace: Trace): RIO[GreyhoundMetrics, DelayedRebalanceEffect] =
        ZIO.succeed(DelayedRebalanceEffect(consumer.commitSync(kafkaOffsetsAndMetaData(offsets))))

      override def pause(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[Any, IllegalStateException, Unit] =
        withConsumer(_.pause(kafkaPartitions(partitions))).refineOrDie { case e: IllegalStateException => e }

      override def resume(partitions: Set[TopicPartition])(implicit trace: Trace): ZIO[Any, IllegalStateException, Unit] =
        withConsumer(consumer => {
          val onlySubscribed = consumer.assignment().asScala.toSet intersect kafkaPartitions(partitions).asScala.toSet
          consumer.resume(onlySubscribed.asJavaCollection)
        }).refineOrDie { case e: IllegalStateException => e }

      override def seek(partition: TopicPartition, offset: Offset)(implicit trace: Trace): ZIO[Any, IllegalStateException, Unit] =
        withConsumerM(seekUnsafe(Seq(partition -> offset)))

      private def seekUnsafe(positions: Iterable[(TopicPartition, Offset)])(c: KafkaConsumer[Chunk[Byte], Chunk[Byte]]) = {
        ZIO.attempt(positions.foreach { case (partition, offset) => c.seek(partition.asKafka, offset) }).refineOrDie {
          case e: IllegalStateException => e
        }
      }

      override def position(topicPartition: TopicPartition)(implicit trace: Trace): Task[Offset] =
        withConsumer(_.position(topicPartition.asKafka))

      override def assignment(implicit trace: Trace): Task[Set[TopicPartition]] = {
        withConsumer(_.assignment().asScala.toSet.map(TopicPartition.apply(_: org.apache.kafka.common.TopicPartition)))
      }

      override def assign(tps: Set[TopicPartition])(implicit trace: Trace): Task[Unit] =
        withConsumer(_.assign(kafkaPartitions(tps)))

      private def allPositionsUnsafe = attemptBlocking {
        consumer
          .assignment()
          .asScala
          .toSet
          .map((tp: KafkaTopicPartition) => TopicPartition(tp) -> consumer.position(tp))
          .toMap
      }

      override def config(implicit trace: Trace): ConsumerConfig = cfg

      private def withConsumer[A](f: KafkaConsumer[Chunk[Byte], Chunk[Byte]] => A): Task[A] =
        semaphore.withPermit(ZIO.attempt(f(consumer)))

      private def withConsumerBlocking[A](f: KafkaConsumer[Chunk[Byte], Chunk[Byte]] => A): RIO[Any, A] =
        semaphore.withPermit(attemptBlocking(f(consumer)))

      private def withConsumerM[R, A, E](f: KafkaConsumer[Chunk[Byte], Chunk[Byte]] => ZIO[R, E, A]): ZIO[R, E, A] =
        semaphore.withPermit(f(consumer))

      override def offsetsForTimes(
        topicPartitionsOnTimestamp: Map[TopicPartition, Long]
      )(implicit trace: Trace): RIO[Any, Map[TopicPartition, Offset]] = {
        val kafkaTopicPartitionsOnTimestamp = topicPartitionsOnTimestamp.map { case (tp, ts) => tp.asKafka -> ts }
        withConsumerBlocking(_.offsetsForTimes(kafkaTopicPartitionsOnTimestamp.mapValues(l => new lang.Long(l)).toMap.asJava))
          .map(
            _.asScala
              .filter { case (_, offset) => offset != null }
              .map { case (ktp, offset) => TopicPartition(ktp) -> offset.offset() }
              .toMap
          )
      }

      override def listTopics(implicit trace: Trace): RIO[Any, Map[Topic, List[PartitionInfo]]] =
        withConsumer(_.listTopics()).map { topics => topics.asScala.mapValues(_.asScala.toList.map(PartitionInfo.apply)).toMap }

      override def shutdown(timeout: Duration)(implicit trace: Trace): Task[Unit] =
        withConsumer(_.close(java.time.Duration.ofMillis(timeout.toMillis)))
          .reporting(ClosedConsumer(config.groupId, config.clientId, _))
          .provideEnvironment(metrics)
    }
  }

  case class InitialOffsetsAndMetadata(offsetsAndMetadata: Map[TopicPartition, OffsetAndMetadata]) extends com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric

  private def listener[R1](consumer: Consumer, onAssignFirstDo: Set[TopicPartition] => Unit, rebalanceListener: RebalanceListener[R1], unsafeOffsetOperations: UnsafeOffsetOperations) =
    ZIO.runtime[R1 with GreyhoundMetrics].map { runtime =>
      new ConsumerRebalanceListener {

        def reportInitialOffsetsAndMetadata(partitions: Set[TopicPartition]) = {
          val offsetsAndMetadata = unsafeOffsetOperations.committedWithMetadata(partitions, 10.seconds)
          report(InitialOffsetsAndMetadata(offsetsAndMetadata))
        }

        override def onPartitionsRevoked(partitions: util.Collection[KafkaTopicPartition]): Unit = {
          zio.Unsafe.unsafe { implicit s =>
            runtime.unsafe
              .run(
                rebalanceListener.onPartitionsRevoked(consumer, partitionsFor(partitions))
              )
              .getOrThrowFiberFailure()
              .run()
          }
          //          runtime
          //            .unsafeRun()
          //            .run() // this needs to be run in the same thread
        }

        override def onPartitionsAssigned(partitions: util.Collection[KafkaTopicPartition]): Unit = {
          val assigned = partitionsFor(partitions)
          onAssignFirstDo(assigned)
          zio.Unsafe.unsafe { implicit s =>
            runtime.unsafe
              .run(
                reportInitialOffsetsAndMetadata(assigned) *> rebalanceListener.onPartitionsAssigned(consumer, assigned)
              )
              .getOrThrowFiberFailure()
              .run()
          }
        }

        private def partitionsFor(partitions: util.Collection[KafkaTopicPartition]) =
          partitions.asScala.map(TopicPartition(_)).toSet
      }
    }

  private def makeConsumer(
    config: ConsumerConfig,
    semaphore: Semaphore
  )(implicit trace: Trace): RIO[GreyhoundMetrics with Scope, KafkaConsumer[Chunk[Byte], Chunk[Byte]]] = {
    val acquire                              = ZIO.attemptBlocking(new KafkaConsumer(config.properties, deserializer, deserializer))
    def close(consumer: KafkaConsumer[_, _]) =
      attemptBlocking(consumer.close())
        .reporting(ClosedConsumer(config.groupId, config.clientId, _))
        .ignore

    ZIO.acquireRelease(acquire)(consumer => semaphore.withPermit(close(consumer)))
  }

}

case class ConsumerConfig(
  bootstrapServers: String,
  groupId: Group,
  clientId: ClientId = s"wix-consumer-${Random.alphanumeric.take(5).mkString}",
  offsetReset: OffsetReset = OffsetReset.Latest,
  extraProperties: Map[String, String] = Map.empty,
  additionalListener: RebalanceListener[Any] = RebalanceListener.Empty,
  initialSeek: InitialOffsetsSeek = InitialOffsetsSeek.default,
  consumerAttributes: Map[String, String] = Map.empty,
  decryptor: Decryptor[Any, Throwable, Chunk[Byte], Chunk[Byte]] = new NoOpDecryptor,
  commitMetadataString: Metadata = OffsetAndMetadata.NO_METADATA,
  rewindUncommittedOffsetsByMillis: Long = 0L,
  useParallelConsumer: Boolean = false,
  enrichMetadata: Boolean = false
) extends CommonGreyhoundConfig {

  override def kafkaProps: Map[String, String] = Map(
    KafkaConsumerConfig.BOOTSTRAP_SERVERS_CONFIG  -> bootstrapServers,
    KafkaConsumerConfig.GROUP_ID_CONFIG           -> groupId,
    KafkaConsumerConfig.CLIENT_ID_CONFIG          -> clientId,
    (
      KafkaConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
      offsetReset match {
        case OffsetReset.Earliest => "earliest"
        case OffsetReset.Latest   => "latest"
      }
    ),
    KafkaConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  ) ++ extraProperties

  def offsetResetIsEarliest: Boolean =
    extraProperties.get("auto.offset.reset").map(_ == "earliest").getOrElse(offsetReset == OffsetReset.Earliest)

  def withExtraProperties(props: (String, String)*) =
    copy(extraProperties = extraProperties ++ props)

}

sealed trait OffsetReset

object OffsetReset {

  case object Earliest extends OffsetReset

  case object Latest extends OffsetReset

}

trait UnsafeOffsetOperations {
  def committed(partitions: Set[TopicPartition], timeout: zio.Duration): Map[TopicPartition, Offset]

  def committedWithMetadata(partitions: Set[TopicPartition], timeout: zio.Duration): Map[TopicPartition, OffsetAndMetadata]

  def beginningOffsets(partitions: Set[TopicPartition], timeout: zio.Duration): Map[TopicPartition, Offset]

  def position(partition: TopicPartition, timeout: zio.Duration): Offset

  def commit(offsets: Map[TopicPartition, Offset], timeout: Duration): Unit

  def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: Duration): Unit

  def seek(offsets: Map[TopicPartition, Offset]): Unit

  def endOffsets(partitions: Set[TopicPartition], timeout: Duration): Map[TopicPartition, Offset]

  def offsetsForTimes(partitions: Set[TopicPartition], timeEpoch: Long, timeout: Duration): Map[TopicPartition, Option[Long]]

  def pause(partitions: Set[TopicPartition]): Unit

  def resume(partitions: Set[TopicPartition]): Unit
}

object UnsafeOffsetOperations {
  def make(consumer: KafkaConsumer[_, _]) = new UnsafeOffsetOperations {

    override def pause(partitions: Set[TopicPartition]): Unit =
      consumer.pause(partitions.map(_.asKafka).asJava)

    override def resume(partitions: Set[TopicPartition]): Unit =
      consumer.resume(partitions.map(_.asKafka).asJava)

    override def committed(partitions: Set[TopicPartition], timeout: Duration): Map[TopicPartition, Offset] = {
      consumer
        .committed(partitions.map(_.asKafka).asJava, timeout)
        .asScala
        .toMap
        .collect {
          case (tp, ofm) if ofm != null =>
            TopicPartition(tp) -> ofm.offset()
        }
    }

    override def committedWithMetadata(
      partitions: NonEmptySet[TopicPartition],
      timeout: zio.Duration
    ): Map[TopicPartition, OffsetAndMetadata] = {
      consumer
        .committed(partitions.map(_.asKafka).asJava, timeout)
        .asScala
        .toMap
        .collect {
          case (tp, ofm) if ofm != null =>
            TopicPartition(tp) -> OffsetAndMetadata(ofm.offset(), ofm.metadata())
        }
    }

    override def beginningOffsets(partitions: Set[TopicPartition], timeout: Duration): Map[TopicPartition, Offset] =
      consumer
        .beginningOffsets(partitions.map(_.asKafka).asJava, timeout)
        .asScala
        .toMap
        .collect {
          case (tp, offset) if offset != null =>
            TopicPartition(tp) -> offset.toLong
        }

    override def position(partition: TopicPartition, timeout: Duration): Offset =
      consumer.position(partition.asKafka, timeout)

    override def commit(offsets: Map[TopicPartition, Offset], timeout: Duration): Unit = {
      consumer.commitSync(kafkaOffsets(offsets), timeout)
    }

    override def commitWithMetadata(offsets: Map[TopicPartition, OffsetAndMetadata], timeout: zio.Duration): Unit = {
      consumer.commitSync(kafkaOffsetsAndMetaData(offsets), timeout)
    }

    override def seek(offsets: Map[TopicPartition, Offset]): Unit =
      offsets.foreach { case (tp, offset) => Try(consumer.seek(tp.asKafka, offset)) }

    override def endOffsets(partitions: Set[TopicPartition], timeout: Duration): Map[TopicPartition, Long] = {
      consumer.endOffsets(partitions.map(_.asKafka).asJava, timeout).asScala.toMap.map { case (tp, of) => TopicPartition(tp) -> (of: Long) }
    }

    override def offsetsForTimes(partitions: Set[TopicPartition], timeEpoch: Long, timeout: Duration): Map[TopicPartition, Option[Long]] =
      consumer
        .offsetsForTimes(partitions.map(_.asKafka).map(tp => (tp, new lang.Long(timeEpoch))).toMap.asJava, timeout)
        .asScala
        .toMap
        .map { case (tp, of) => TopicPartition(tp) -> (Option(of).map(_.offset())) }
  }
}
