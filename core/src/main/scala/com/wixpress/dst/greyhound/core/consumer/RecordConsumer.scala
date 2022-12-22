package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern
import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import com.wixpress.dst.greyhound.core.consumer.ConsumerConfigFailedValidation.InvalidRetryConfigForPatternSubscription
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.CreatingConsumer
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.consumer.RecordConsumerMetric.{ResubscribeError, UncaughtHandlerError}
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerSubscription, Decryptor, NoOpDecryptor, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper.{patternRetryTopic, retryPattern}
import com.wixpress.dst.greyhound.core.consumer.retry._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRetryPolicy, ReportingProducer}
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown.ShutdownPromise
import zio._

import scala.util.Random

trait RecordConsumerProperties[+STATE] {
  def group(implicit trace: Trace): Group

  def clientId(implicit trace: Trace): ClientId

  def state(implicit trace: Trace): UIO[STATE]

  def topology(implicit trace: Trace): UIO[RecordConsumerTopology]
}

trait RecordConsumer[-R] extends Resource[R] with RecordConsumerProperties[RecordConsumerExposedState] {
  def shutdown(): RIO[Env, Unit]

  def resubscribe[R1: Tag](
    subscription: ConsumerSubscription,
    listener: RebalanceListener[R1] = RebalanceListener.Empty
  ): RIO[Env with R1, AssignedPartitions]

  def setBlockingState(command: BlockingStateCommand): RIO[Env, Unit]

  def endOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]]

  def beginningOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]]

  def committedOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]]

  def waitForCurrentRecordsCompletion: URIO[Any, Unit]

  def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Any, Map[TopicPartition, Offset]]

  def seek[R1](toOffsets: Map[TopicPartition, Offset]): RIO[Env with R1, Unit]
}

object RecordConsumer {
  type Env                = GreyhoundMetrics
  type AssignedPartitions = Set[TopicPartition]

  /**
   * Creates a RecordConsumer, that when used will start consuming messages from Kafka and invoke the appropriate handlers. Handling is
   * concurrent between partitions; order is guaranteed to be maintained within the same partition.
   */
  def make[R, E](
    config: RecordConsumerConfig,
    handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]]
  )(implicit trace: Trace, tag: Tag[Env]): ZIO[R with Env with Scope with GreyhoundMetrics, Throwable, RecordConsumer[R with Env]] =
    ZIO
      .acquireRelease(
        for {
          consumerShutdown <- AwaitShutdown.make
          _                <- GreyhoundMetrics
                                .report(CreatingConsumer(config.clientId, config.group, config.bootstrapServers, config.consumerAttributes))

          _                                    <- validateRetryPolicy(config)
          consumerSubscriptionRef              <- Ref.make[ConsumerSubscription](config.initialSubscription)
          nonBlockingRetryHelper                = NonBlockingRetryHelper(config.group, config.retryConfig)
          consumer                             <- Consumer.make(consumerConfig(config))
          (initialSubscription, topicsToCreate) = config.retryConfig.fold((config.initialSubscription, Set.empty[Topic]))(policy =>
                                                    maybeAddRetryTopics(policy, config, nonBlockingRetryHelper)
                                                  )
          _                                    <- AdminClient
                                                    .make(AdminClientConfig(config.bootstrapServers, config.kafkaAuthProperties), config.consumerAttributes)
                                                    .tap(client =>
                                                      client.createTopics(
                                                        topicsToCreate.map(topic =>
                                                          TopicConfig(topic, partitions = 1, replicationFactor = 1, cleanupPolicy = CleanupPolicy.Delete(86400000L))
                                                        )
                                                      )
                                                    )
          blockingState                        <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty)
          blockingStateResolver                 = BlockingStateResolver(blockingState)
          workersShutdownRef                   <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
          combinedAwaitShutdown                 = combineAwaitShutdowns(consumerShutdown, workersShutdownRef)
          handlerWithRetries                   <- addRetriesToHandler(config, handler, blockingState, nonBlockingRetryHelper, combinedAwaitShutdown)
          eventLoop                            <- EventLoop.make(
                                                    group = config.group,
                                                    initialSubscription = initialSubscription,
                                                    consumer = ReportingConsumer(config.clientId, config.group, consumer),
                                                    handler = handlerWithRetries,
                                                    config = config.eventLoopConfig,
                                                    clientId = config.clientId,
                                                    consumerAttributes = config.consumerAttributes,
                                                    workersShutdownRef = workersShutdownRef
                                                  )
        } yield new RecordConsumer[R with Env] {
          override def shutdown(): RIO[Env, Unit] =
            consumerShutdown.onShutdown.shuttingDown *> eventLoop.stop.unit *> consumer.shutdown(30.seconds)

          override def pause(implicit trace: Trace): URIO[R with Env, Unit] =
            eventLoop.pause

          override def resume(implicit trace: Trace): URIO[R with Env, Unit] =
            eventLoop.resume

          override def isAlive(implicit trace: Trace): URIO[R with Env, Boolean] =
            eventLoop.isAlive

          override def setBlockingState(command: BlockingStateCommand): RIO[Env, Unit] = {
            blockingStateResolver.setBlockingState(command)
          }

          override def endOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]] =
            consumer.endOffsets(partitions)

          override def beginningOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]] =
            consumer.beginningOffsets(partitions)

          override def committedOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]] =
            consumer.committedOffsets(partitions)

          override def waitForCurrentRecordsCompletion: URIO[Any, Unit] = eventLoop.waitForCurrentRecordsCompletion

          override def state(implicit trace: Trace): UIO[RecordConsumerExposedState] = for {
            elState          <- eventLoop.state
            blockingStateMap <- blockingState.get
          } yield RecordConsumerExposedState(elState, config.clientId, blockingStateMap)

          override def topology(implicit trace: Trace): UIO[RecordConsumerTopology] =
            consumerSubscriptionRef.get.map(subscription => RecordConsumerTopology(config.group, subscription, config.consumerAttributes))

          override def group(implicit trace: Trace): Group = config.group

          override def resubscribe[R1: Tag](
            subscription: ConsumerSubscription,
            listener: RebalanceListener[R1]
          ): RIO[Env with R1, AssignedPartitions] =
            for {
              assigned         <- Ref.make[AssignedPartitions](Set.empty)
              promise          <- Promise.make[Nothing, AssignedPartitions]
              rebalanceListener = eventLoop.rebalanceListener *> listener *>
                                    new RebalanceListener[R1] {
                                      override def onPartitionsRevoked(
                                        consumer: Consumer,
                                        partitions: Set[TopicPartition]
                                      )(implicit trace: Trace): URIO[R1, DelayedRebalanceEffect] =
                                        DelayedRebalanceEffect.zioUnit

                                      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
                                        implicit trace: Trace
                                      ): URIO[R1, Any] =
                                        for {
                                          allAssigned <- assigned.updateAndGet(_ => partitions)
                                          _           <- consumerSubscriptionRef.set(subscription)
                                          _           <- promise.succeed(allAssigned)
                                        } yield ()
                                    }

              _                 <- subscribe[R1](subscription, rebalanceListener)(consumer)
              resubscribeTimeout = config.eventLoopConfig.drainTimeout
              result            <- promise.await.disconnect
                                     .timeoutFail(ResubscribeTimeout(resubscribeTimeout, subscription))(resubscribeTimeout)
                                     .catchAll(ex => report(ResubscribeError(ex, group, clientId)) *> ZIO.succeed(Set.empty[TopicPartition]))
            } yield result

          override def clientId(implicit trace: Trace): ClientId = config.clientId

          override def offsetsForTimes(
            topicPartitionsOnTimestamp: Map[TopicPartition, Long]
          ): RIO[Any, Map[TopicPartition, Offset]] =
            consumer.offsetsForTimes(topicPartitionsOnTimestamp)

          override def seek[R1](toOffsets: Map[TopicPartition, Offset]): RIO[Env with R1, Unit] =
            consumer.seek(toOffsets)
        }
      )(_.shutdown().catchAllCause(e => ZIO.succeed(e.squashTrace.printStackTrace())))

  private def combineAwaitShutdowns(consumerShutdown: ShutdownPromise, workersShutdownRef: Ref[Map[TopicPartition, ShutdownPromise]])(
    implicit trace: Trace
  ) =
    (tp: TopicPartition) => {
      workersShutdownRef.get.map(workers =>
        workers.get(tp).fold(consumerShutdown.awaitShutdown)(_.awaitShutdown or consumerShutdown.awaitShutdown)
      )
    }

  def consumerConfig[E, R](config: RecordConsumerConfig) = {
    ConsumerConfig(
      config.bootstrapServers,
      config.group,
      config.clientId,
      config.offsetReset,
      config.extraProperties,
      config.userProvidedListener,
      config.initialOffsetsSeek,
      config.consumerAttributes,
      config.decryptor,
      config.commitMetadataString,
      config.rewindUncommittedOffsetsBy.toMillis
    )
  }

  private def maybeAddRetryTopics[E, R](
    retryConfig: RetryConfig,
    config: RecordConsumerConfig,
    helper: NonBlockingRetryHelper
  )(implicit trace: Trace): (ConsumerSubscription, Set[String]) = {
    config.initialSubscription match {
      case Topics(topics)           =>
        val retryTopics = topics.flatMap(helper.retryTopicsFor)
        (Topics(topics ++ retryTopics), retryTopics)
      case TopicPattern(pattern, _) =>
        (
          TopicPattern(Pattern.compile(s"${pattern.pattern}|${retryPattern(config.group)}")),
          (0 until retryConfig.nonBlockingBackoffs("").length)
            .map(step => patternRetryTopic(config.group, step))
            .toSet
        )
    }
  }

  private def addRetriesToHandler[R, E](
    config: RecordConsumerConfig,
    handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]],
    blockingState: Ref[Map[BlockingTarget, BlockingState]],
    nonBlockingRetryHelper: NonBlockingRetryHelper,
    awaitShutdown: TopicPartition => UIO[AwaitShutdown]
  )(implicit trace: Trace) =
    config.retryConfig match {
      case Some(retryConfig) =>
        Producer
          .makeR[R](
            ProducerConfig(
              config.bootstrapServers,
              retryPolicy = ProducerRetryPolicy(Int.MaxValue, 3.seconds),
              extraProperties = config.kafkaAuthProperties + ("max.request.size" -> "8000000")
            )
          )
          .map(producer => ReportingProducer(producer, config.retryProducerAttributes))
          .map(producer =>
            RetryRecordHandler.withRetries(
              config.group,
              handler,
              retryConfig,
              producer,
              config.initialSubscription,
              blockingState,
              nonBlockingRetryHelper,
              awaitShutdown
            )
          )
      case None              =>
        ZIO.succeed(
          handler.withErrorHandler((e, record) =>
            report(UncaughtHandlerError(e, record.topic, record.partition, record.offset, config.group, config.clientId))
          )
        )
    }

  private def validateRetryPolicy(config: RecordConsumerConfig) =
    config.initialSubscription match {
      case _: ConsumerSubscription.TopicPattern =>
        ZIO.unit
      case _: ConsumerSubscription.Topics       =>
        ZIO.when(config.retryConfig.exists(_.forPatternSubscription.exists(_.nonEmpty)))(ZIO.fail(InvalidRetryConfigForPatternSubscription))
    }

}

sealed trait RecordConsumerMetric extends GreyhoundMetric {
  def group: Group

  def clientId: ClientId
}

object RecordConsumerMetric {

  case class UncaughtHandlerError[E](error: E, topic: Topic, partition: Partition, offset: Offset, group: Group, clientId: ClientId)
      extends RecordConsumerMetric

  case class ResubscribeError[E](error: E, group: Group, clientId: ClientId) extends RecordConsumerMetric

}

case class RecordConsumerExposedState(
  eventLoopState: EventLoopExposedState,
  consumerId: String,
  blockingState: Map[BlockingTarget, BlockingState]
) {
  /* List of consumed topics so far */
  def topics = eventLoopState.dispatcherState.topics

  /* The latest offset submitted for execution per topic-partition */
  def eventLoopLatestOffsets = eventLoopState.latestOffsets
}

case class RecordConsumerTopology(group: Group, subscription: ConsumerSubscription, attributes: Map[String, String] = Map.empty)

case class RecordConsumerConfig(
  bootstrapServers: String,
  group: Group,
  initialSubscription: ConsumerSubscription,
  retryConfig: Option[RetryConfig] = None,
  clientId: String = RecordConsumerConfig.makeClientId,
  eventLoopConfig: EventLoopConfig = EventLoopConfig.Default,
  offsetReset: OffsetReset = OffsetReset.Latest,
  extraProperties: Map[String, String] = Map.empty,
  userProvidedListener: RebalanceListener[Any] = RebalanceListener.Empty,
  initialOffsetsSeek: InitialOffsetsSeek = InitialOffsetsSeek.default,
  consumerAttributes: Map[String, String] = Map.empty,
  decryptor: Decryptor[Any, Throwable, Chunk[Byte], Chunk[Byte]] = new NoOpDecryptor,
  retryProducerAttributes: Map[String, String] = Map.empty,
  commitMetadataString: Metadata = OffsetAndMetadata.NO_METADATA,
  rewindUncommittedOffsetsBy: Duration = 0.millis
) extends CommonGreyhoundConfig {

  override def kafkaProps: Map[String, String] = extraProperties
}

object RecordConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}

case class ResubscribeTimeout(resubscribeTimeout: Duration, subscription: ConsumerSubscription)
    extends RuntimeException(s"Resubscribe timeout (${resubscribeTimeout.getSeconds} s) for $subscription")

abstract class ConsumerConfigFailedValidation(val msg: String) extends RuntimeException(msg)

object ConsumerConfigFailedValidation {

  case object InvalidRetryConfigForPatternSubscription
      extends ConsumerConfigFailedValidation(
        "A consumer with a pattern subscription cannot be created with a custom retry policy. Use ZRetryConfig.retryForPattern(..)"
      )

}
