package com.wixpress.dst.greyhound.core.consumer.batched

import java.time.Duration

import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.consumer.{EventLoopExposedState => _, _}
import com.wixpress.dst.greyhound.core.consumer.domain.{BatchRecordHandler, ConsumerRecordBatch, ConsumerSubscription, Decryptor, NoOpDecryptor}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{ClientId, Group, Offset, TopicPartition}
import zio.duration._
import zio.{duration, Chunk, Has, Promise, RIO, Ref, UIO, URIO, ZEnv, ZManaged}

import scala.reflect.ClassTag
import scala.util.Random

case class EffectiveConfig(consumerConfig: ConsumerConfig, batchConsumerConfig: BatchConsumerConfig)

trait BatchConsumer[-R] extends Resource[R] with RecordConsumerProperties[BatchConsumerExposedState] {
  def resubscribe[R1](
    subscription: ConsumerSubscription,
    listener: RebalanceListener[R1] = RebalanceListener.Empty
  ): RIO[Env with R1, AssignedPartitions]

  def seek[R1](toOffsets: Map[TopicPartition, Offset]): RIO[Env with R1, Unit]

  def offsetsForTimes[R1](topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Env with R1, Map[TopicPartition, Offset]]

  def effectiveConfig: EffectiveConfig

  def endOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]]

  def committedOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]]
}

object BatchConsumer {

  type Env                = ZEnv with GreyhoundMetrics
  type AssignedPartitions = Set[TopicPartition]
  type RecordBatch        = ConsumerRecordBatch[Chunk[Byte], Chunk[Byte]]

  def make[R <: Has[_]: ClassTag](
    config: BatchConsumerConfig,
    handler: BatchRecordHandler[R, Any, Chunk[Byte], Chunk[Byte]]
  ): ZManaged[R with Env, Throwable, BatchConsumer[R]] = for {
    consumerSubscriptionRef <- Ref.make[ConsumerSubscription](config.initialSubscription).toManaged_
    assignments             <- Ref.make(Set.empty[TopicPartition]).toManaged_
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
    override def group: Group = config.groupId

    override def clientId: ClientId = config.clientId

    override def state: UIO[BatchConsumerExposedState] = assignments.get.zipWith(eventLoop.state) {
      case (partitions, evs) =>
        BatchConsumerExposedState(evs, clientId, partitions)
    }

    override def topology: UIO[RecordConsumerTopology] = consumerSubscriptionRef.get.map(RecordConsumerTopology(group, _))

    override def resubscribe[R1](
      subscription: ConsumerSubscription,
      listener: RebalanceListener[R1]
    ): RIO[Env with R1, AssignedPartitions] =
      for {
        assigned          <- Ref.make[AssignedPartitions](Set.empty)
        promise           <- Promise.make[Nothing, AssignedPartitions]
        rebalanceListener  = eventLoop.rebalanceListener *> listener *>
                               new RebalanceListener[R1] {
                                 override def onPartitionsRevoked(
                                   consumer: Consumer,
                                   partitions: Set[TopicPartition]
                                 ): URIO[R1, DelayedRebalanceEffect] =
                                   DelayedRebalanceEffect.zioUnit

                                 override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): URIO[R1, Any] =
                                   for {
                                     allAssigned <- assigned.updateAndGet(_ => partitions)
                                     _           <- consumerSubscriptionRef.set(subscription)
                                     _           <- promise.succeed(allAssigned)
                                   } yield ()
                               }
        _                 <- subscribe[R1](subscription, rebalanceListener)(consumer)
        resubscribeTimeout = config.resubscribeTimeout
        result            <- promise.await.disconnect
                               .timeoutFail(BatchResubscribeTimeout(resubscribeTimeout, subscription))(resubscribeTimeout)
                               .catchAll(_ => UIO(Set.empty[TopicPartition]))
      } yield result

    override def pause: URIO[R, Unit] = eventLoop.pause

    override def resume: URIO[R, Unit] = eventLoop.resume

    override def isAlive: URIO[R, Boolean] = eventLoop.isAlive

    override def effectiveConfig: EffectiveConfig = EffectiveConfig(consumer.config, config)

    override def seek[R1](toOffsets: Map[TopicPartition, Offset]): RIO[Env with R1, Unit] =
      eventLoop.requestSeek(toOffsets)

    override def offsetsForTimes[R1](topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Env with R1, Map[TopicPartition, Offset]] =
      consumer.offsetsForTimes(topicPartitionsOnTimestamp)

    override def endOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]] =
      consumer.endOffsets(partitions)

    override def beginningOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]] =
      consumer.beginningOffsets(partitions)

    override def committedOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]] =
      consumer.committedOffsets(partitions)
  }

  private def consumerConfig[R <: Has[_]: ClassTag](config: BatchConsumerConfig, assignmentsListener: RebalanceListener[Any]) = {
    ConsumerConfig(
      config.bootstrapServers,
      config.groupId,
      config.clientId,
      config.offsetReset,
      config.extraProperties,
      assignmentsListener *> config.userProvidedListener,
      config.initialOffsetsSeek,
      config.consumerAttributes,
      config.decryptor
    )
  }

  private def trackAssignments(assignments: Ref[Set[TopicPartition]]) = {
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition]): URIO[Any, DelayedRebalanceEffect] =
        assignments.update(_ -- partitions).as(DelayedRebalanceEffect.unit)

      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): URIO[Any, Any] =
        assignments.update(_ ++ partitions)
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
  decryptor: Decryptor[Any, Throwable, Chunk[Byte], Chunk[Byte]] = new NoOpDecryptor
)

object BatchConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}

final case class BatchRetryConfig private[greyhound] (backoff: Duration)

object BatchRetryConfig {
  def infiniteBlockingRetry(backoff: scala.concurrent.duration.Duration): BatchRetryConfig =
    BatchRetryConfig(zio.duration.Duration.fromScala(backoff))

}

case class BatchResubscribeTimeout(resubscribeTimeout: duration.Duration, subscription: ConsumerSubscription)
    extends RuntimeException(s"Resubscribe timeout (${resubscribeTimeout.getSeconds} s) for $subscription")
