package com.wixpress.dst.greyhound.core.consumer.batched

import java.time.Duration
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.consumer.{EventLoopExposedState => _, _}
import com.wixpress.dst.greyhound.core.consumer.domain.{BatchRecordHandler, ConsumerRecordBatch, ConsumerSubscription, Decryptor, NoOpDecryptor}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{ClientId, Group, Metadata, Offset, OffsetAndMetadata, TopicPartition}
import zio.{Chunk, Promise, RIO, Ref, UIO, URIO}

import scala.reflect.ClassTag
import scala.util.Random
import zio._

case class EffectiveConfig(consumerConfig: ConsumerConfig, batchConsumerConfig: BatchConsumerConfig)

trait BatchConsumer[-R] extends Resource[R] with RecordConsumerProperties[BatchConsumerExposedState] {
  def shutdown(timeout: Duration)(implicit trace: Trace): Task[Unit]

  def resubscribe[R1](
    subscription: ConsumerSubscription,
    listener: RebalanceListener[R1] = RebalanceListener.Empty
  )(implicit trace: Trace, tag: Tag[R1]): RIO[Env with R1, AssignedPartitions]

  def seek[R1](toOffsets: Map[TopicPartition, Offset])(implicit trace: Trace): RIO[Env with R1, Unit]

  def offsetsForTimes[R1](topicPartitionsOnTimestamp: Map[TopicPartition, Long])(
    implicit trace: Trace
  ): RIO[Env with R1, Map[TopicPartition, Offset]]

  def effectiveConfig(implicit trace: Trace): EffectiveConfig

  def endOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Env, Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Env, Map[TopicPartition, Offset]]

  def committedOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Env, Map[TopicPartition, Offset]]
}

object BatchConsumer {

  type Env                = GreyhoundMetrics
  type AssignedPartitions = Set[TopicPartition]
  type RecordBatch        = ConsumerRecordBatch[Chunk[Byte], Chunk[Byte]]

  def make[R](
    config: BatchConsumerConfig,
    handler: BatchRecordHandler[R, Any, Chunk[Byte], Chunk[Byte]]
  )(implicit trace: Trace): ZIO[R with Env with Scope, Throwable, BatchConsumer[R]] = for {
    consumerSubscriptionRef <- Ref.make[ConsumerSubscription](config.initialSubscription)
    assignments             <- Ref.make(Set.empty[TopicPartition])
    assignmentsListener      = trackAssignments(assignments)
    consumer                <- Consumer.make(consumerConfig(config, assignmentsListener))
    eventLoop               <- BatchEventLoop.make[R](
                                 config.groupId,
                                 config.initialSubscription,
                                 ReportingConsumer(config.clientId, config.groupId, consumer),
                                 handler,
                                 config.clientId,
                                 config.retryConfig,
                                 config.eventLoopConfig
                               )
  } yield new BatchConsumer[R] {

    override def group(implicit trace: Trace): Group = config.groupId

    override def clientId(implicit trace: Trace): ClientId = config.clientId

    override def state(implicit trace: Trace): UIO[BatchConsumerExposedState] = assignments.get.zipWith(eventLoop.state) {
      case (partitions, evs) =>
        BatchConsumerExposedState(evs, clientId, partitions)
    }

    override def topology(implicit trace: Trace): UIO[RecordConsumerTopology] =
      consumerSubscriptionRef.get.map(RecordConsumerTopology(group, _))

    override def resubscribe[R1](
      subscription: ConsumerSubscription,
      listener: RebalanceListener[R1]
    )(implicit trace: Trace, tag: Tag[R1]): RIO[Env with R1, AssignedPartitions] =
      for {
        assigned          <- Ref.make[AssignedPartitions](Set.empty)
        promise           <- Promise.make[Nothing, AssignedPartitions]
        rebalanceListener  = eventLoop.rebalanceListener *> listener *>
                               new RebalanceListener[R1] {
                                 override def onPartitionsRevoked(
                                   consumer: Consumer,
                                   partitions: Set[TopicPartition]
                                 )(implicit trace: Trace): URIO[R1, DelayedRebalanceEffect] =
                                   DelayedRebalanceEffect.zioUnit

                                 override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
                                   implicit trace: Trace
                                 ): URIO[R1, DelayedRebalanceEffect] =
                                   for {
                                     allAssigned <- assigned.updateAndGet(_ => partitions)
                                     _           <- consumerSubscriptionRef.set(subscription)
                                     _           <- promise.succeed(allAssigned)
                                   } yield DelayedRebalanceEffect.unit
                               }
        _                 <- subscribe[R1](subscription, rebalanceListener)(consumer)
        resubscribeTimeout = config.resubscribeTimeout
        result            <- promise.await.disconnect
                               .timeoutFail(BatchResubscribeTimeout(resubscribeTimeout, subscription))(resubscribeTimeout)
                               .catchAll(_ => ZIO.succeed(Set.empty[TopicPartition]))
      } yield result

    override def pause(implicit trace: Trace): URIO[R, Unit] = eventLoop.pause

    override def resume(implicit trace: Trace): URIO[R, Unit] = eventLoop.resume

    override def isAlive(implicit trace: Trace): URIO[R, Boolean] = eventLoop.isAlive

    override def effectiveConfig(implicit trace: Trace): EffectiveConfig = EffectiveConfig(consumer.config, config)

    override def seek[R1](toOffsets: Map[TopicPartition, Offset])(implicit trace: Trace): RIO[Env with R1, Unit] =
      eventLoop.requestSeek(toOffsets)

    override def offsetsForTimes[R1](topicPartitionsOnTimestamp: Map[TopicPartition, Long])(
      implicit trace: Trace
    ): RIO[Env with R1, Map[TopicPartition, Offset]] =
      consumer.offsetsForTimes(topicPartitionsOnTimestamp)

    override def endOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Env, Map[TopicPartition, Offset]] =
      consumer.endOffsets(partitions)

    override def beginningOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Env, Map[TopicPartition, Offset]] =
      consumer.beginningOffsets(partitions)

    override def committedOffsets(partitions: Set[TopicPartition])(implicit trace: Trace): RIO[Env, Map[TopicPartition, Offset]] =
      consumer.committedOffsets(partitions)

    override def shutdown(timeout: Duration)(implicit trace: Trace): Task[Unit] =
      eventLoop.shutdown *> consumer.shutdown(timeout)
  }

  private def consumerConfig[R <: Any: ClassTag](config: BatchConsumerConfig, assignmentsListener: RebalanceListener[Any]) = {
    ConsumerConfig(
      config.bootstrapServers,
      config.groupId,
      config.clientId,
      config.offsetReset,
      config.extraProperties,
      assignmentsListener *> config.userProvidedListener,
      config.initialOffsetsSeek,
      config.consumerAttributes,
      config.decryptor,
      config.commitMetadataString,
      config.rewindUncommittedOffsetsBy.toMillis
    )
  }

  private def trackAssignments(assignments: Ref[Set[TopicPartition]]) = {
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): URIO[Any, DelayedRebalanceEffect] =
        assignments.update(_ -- partitions).as(DelayedRebalanceEffect.unit)

      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): URIO[Any, DelayedRebalanceEffect] =
        assignments.update(_ ++ partitions).as(DelayedRebalanceEffect.unit)
    }
  }
}

case class BatchConsumerExposedState(eventLoopState: EventLoopExposedState, clientId: ClientId, assignment: Set[TopicPartition]) {
  def topics = assignment.map(_.topic)
}

case class BatchConsumerConfig(
  bootstrapServers: String,
  groupId: Group,
  initialSubscription: ConsumerSubscription,
  retryConfig: Option[BatchRetryConfig] = None,
  clientId: String = RecordConsumerConfig.makeClientId,
  eventLoopConfig: BatchEventLoopConfig = BatchEventLoopConfig.Default,
  offsetReset: OffsetReset = OffsetReset.Latest,
  extraProperties: Map[String, String] = Map.empty,
  userProvidedListener: RebalanceListener[Any] = RebalanceListener.Empty,
  resubscribeTimeout: Duration = 30.seconds,
  initialOffsetsSeek: InitialOffsetsSeek = InitialOffsetsSeek.default,
  consumerAttributes: Map[String, String] = Map.empty,
  decryptor: Decryptor[Any, Throwable, Chunk[Byte], Chunk[Byte]] = new NoOpDecryptor,
  commitMetadataString: Unit => Metadata = _ => OffsetAndMetadata.NO_METADATA,
  rewindUncommittedOffsetsBy: Duration = Duration.ZERO,
)

object BatchConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}

final case class BatchRetryConfig private[greyhound] (backoff: Duration)

object BatchRetryConfig {
  def infiniteBlockingRetry(backoff: scala.concurrent.duration.Duration): BatchRetryConfig =
    BatchRetryConfig(zio.Duration.fromScala(backoff))

}

case class BatchResubscribeTimeout(resubscribeTimeout: zio.Duration, subscription: ConsumerSubscription)
    extends RuntimeException(s"Resubscribe timeout (${resubscribeTimeout.getSeconds} s) for $subscription")
