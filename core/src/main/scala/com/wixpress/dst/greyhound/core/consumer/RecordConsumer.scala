package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import com.wixpress.dst.greyhound.core.consumer.ConsumerConfigFailedValidation.InvalidRetryConfigForPatternSubscription
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.CreatingConsumer
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.consumer.RecordConsumerMetric.{ResubscribeError, UncaughtHandlerError}
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper.{patternRetryTopic, retryPattern}
import com.wixpress.dst.greyhound.core.consumer.retry._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRetryPolicy, ReportingProducer}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.util.Random

trait RecordConsumerProperties[+STATE] {
  def group: Group

  def clientId: ClientId

  def state: Task[STATE]

  def topology: UIO[RecordConsumerTopology]
}

trait RecordConsumer[-R] extends Resource[R] with RecordConsumerProperties[RecordConsumerExposedState] {
  def resubscribe[R1](subscription: ConsumerSubscription, listener: RebalanceListener[R1] = RebalanceListener.Empty): RIO[Env with R1, AssignedPartitions]

  def setBlockingState(command: BlockingStateCommand): RIO[Env, Unit]

  def endOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]]

  def waitForCurrentRecordsCompletion: URIO[Clock, Unit]

  def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Clock with Blocking, Map[TopicPartition, Offset]]

  def seek[R1](toOffsets: Map[TopicPartition, Offset]): RIO[Env with R1, Unit]
}

object RecordConsumer {
  type Env = ZEnv with GreyhoundMetrics
  type AssignedPartitions = Set[TopicPartition]

  /**
   * Creates a RecordConsumer, that when used will start consuming messages
   * from Kafka and invoke the appropriate handlers. Handling is concurrent between
   * partitions; order is guaranteed to be maintained within the same partition.
   */
  def make[R, E](config: RecordConsumerConfig, handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]]): ZManaged[R with Env with GreyhoundMetrics, Throwable, RecordConsumer[R with Env]] =
    for {
      _ <- GreyhoundMetrics.report(CreatingConsumer(config.clientId, config.group, config.bootstrapServers)).toManaged_
      _ <- validateRetryPolicy(config)
      consumerSubscriptionRef <- Ref.make[ConsumerSubscription](config.initialSubscription).toManaged_
      nonBlockingRetryHelper = NonBlockingRetryHelper(config.group, config.retryConfig)
      consumer <- Consumer.make(
        ConsumerConfig(config.bootstrapServers, config.group, config.clientId, config.offsetReset, config.extraProperties, config.userProvidedListener, config.initialOffsetsSeek))
      (initialSubscription, topicsToCreate) = config.retryConfig.fold((config.initialSubscription, Set.empty[Topic]))(policy =>
        maybeAddRetryTopics(policy, config, nonBlockingRetryHelper))
      _ <- AdminClient.make(AdminClientConfig(config.bootstrapServers, config.kafkaAuthProperties)).use(client =>
        client.createTopics(topicsToCreate.map(topic => TopicConfig(topic, partitions = 1, replicationFactor = 1, cleanupPolicy = CleanupPolicy.Delete(86400000L))))
      ).toManaged_
      blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty).toManaged_
      blockingStateResolver = BlockingStateResolver(blockingState)
      handlerWithRetries <- addRetriesToHandler(config, handler, blockingState, nonBlockingRetryHelper)
      eventLoop <- EventLoop.make(
        group = config.group,
        initialSubscription = initialSubscription,
        consumer = ReportingConsumer(config.clientId, config.group, consumer),
        handler = handlerWithRetries,
        config = config.eventLoopConfig,
        clientId = config.clientId)
    } yield new RecordConsumer[R with Env] {
      override def pause: URIO[R with Env, Unit] =
        eventLoop.pause

      override def resume: URIO[R with Env, Unit] =
        eventLoop.resume

      override def isAlive: URIO[R with Env, Boolean] =
        eventLoop.isAlive

      override def setBlockingState(command: BlockingStateCommand): RIO[Env, Unit] = {
        blockingStateResolver.setBlockingState(command)
      }

      override def endOffsets(partitions: Set[TopicPartition]): RIO[Env, Map[TopicPartition, Offset]] =
        consumer.endOffsets(partitions)

      override def waitForCurrentRecordsCompletion: URIO[Clock, Unit] = eventLoop.waitForCurrentRecordsCompletion

      override def state: UIO[RecordConsumerExposedState] = for {
        elState <- eventLoop.state
        blockingStateMap <- blockingState.get
      } yield RecordConsumerExposedState(elState, config.clientId, blockingStateMap)

      override def topology: UIO[RecordConsumerTopology] =
        consumerSubscriptionRef.get.map(subscription => RecordConsumerTopology(config.group, subscription))

      override def group: Group = config.group

      override def resubscribe[R1](subscription: ConsumerSubscription, listener: RebalanceListener[R1]): RIO[Env with R1, AssignedPartitions] =
        for {
          assigned <- Ref.make[AssignedPartitions](Set.empty)
          promise <- Promise.make[Nothing, AssignedPartitions]
          rebalanceListener = eventLoop.rebalanceListener *> listener *> new RebalanceListener[R1] {
            override def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[R1, DelayedRebalanceEffect] =
              DelayedRebalanceEffect.zioUnit

            override def onPartitionsAssigned(partitions: Set[TopicPartition]): URIO[R1, Any] = for {
              allAssigned <- assigned.updateAndGet(_ => partitions)
              _ <- consumerSubscriptionRef.set(subscription)
              _ <- promise.succeed(allAssigned)
            } yield ()
          }

          _ <- subscribe[R1](subscription, rebalanceListener)(consumer)
          resubscribeTimeout = config.eventLoopConfig.drainTimeout
          result <- promise.await.disconnect.timeoutFail(
            ResubscribeTimeout(resubscribeTimeout, subscription))(resubscribeTimeout)
            .catchAll(ex => report(ResubscribeError(ex, group, clientId)) *> UIO(Set.empty[TopicPartition]))
        } yield result

      override def clientId: ClientId = config.clientId

      override def offsetsForTimes(topicPartitionsOnTimestamp: Map[TopicPartition, Long]): RIO[Clock with Blocking, Map[TopicPartition, Offset]] =
        consumer.offsetsForTimes(topicPartitionsOnTimestamp)

      override def seek[R1](toOffsets: Map[TopicPartition, Offset]): RIO[Env with R1, Unit] =
        zio.ZIO.foreach(toOffsets.toSeq) { case (tp, offset) => consumer.seek(tp, offset) }.unit
    }

  private def maybeAddRetryTopics[E, R](retryConfig: RetryConfig, config: RecordConsumerConfig, helper: NonBlockingRetryHelper): (ConsumerSubscription, Set[String]) = {
    config.initialSubscription match {
      case Topics(topics) =>
        val retryTopics = topics.flatMap(helper.retryTopicsFor)
        (Topics(topics ++ retryTopics), retryTopics)
      case TopicPattern(pattern, _) => (TopicPattern(Pattern.compile(s"${pattern.pattern}|${retryPattern(config.group)}")),
        (0 until retryConfig.nonBlockingBackoffs("").length)
          .map(step => patternRetryTopic(config.group, step)).toSet)
    }
  }

  private def addRetriesToHandler[R, E](config: RecordConsumerConfig,
                                        handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]],
                                        blockingState: Ref[Map[BlockingTarget, BlockingState]],
                                        nonBlockingRetryHelper: NonBlockingRetryHelper) =
    config.retryConfig match {
      case Some(retryConfig) =>
        Producer.makeR[R](ProducerConfig(config.bootstrapServers,
          retryPolicy = ProducerRetryPolicy(Int.MaxValue, 3.seconds), extraProperties = config.kafkaAuthProperties))
          .map(producer => ReportingProducer(producer))
          .map(producer => RetryRecordHandler.withRetries(config.group, handler, retryConfig, producer, config.initialSubscription, blockingState, nonBlockingRetryHelper))
      case None =>
        ZManaged.succeed(handler.withErrorHandler((e, record) =>
          report(UncaughtHandlerError(e, record.topic, record.partition, record.offset, config.group, config.clientId))))
    }

  private def validateRetryPolicy(config: RecordConsumerConfig) =
    (config.initialSubscription match {
      case _: ConsumerSubscription.TopicPattern =>
        ZIO.unit
      case _: ConsumerSubscription.Topics =>
        ZIO.when(config.retryConfig.exists(_.forPatternSubscription.exists(_.nonEmpty)))(ZIO.fail(InvalidRetryConfigForPatternSubscription))
    }).toManaged_

}

sealed trait RecordConsumerMetric extends GreyhoundMetric {
  def group: Group

  def clientId: ClientId
}

object RecordConsumerMetric {

  case class UncaughtHandlerError[E](error: E, topic: Topic, partition: Partition, offset: Offset, group: Group, clientId: ClientId) extends RecordConsumerMetric

  case class ResubscribeError[E](error: E, group: Group, clientId: ClientId) extends RecordConsumerMetric

}

case class RecordConsumerExposedState(eventLoopState: EventLoopExposedState, consumerId: String,
                                      blockingState: Map[BlockingTarget, BlockingState]) {
  /* List of consumed topics so far */
  def topics = eventLoopState.dispatcherState.topics

  /* The latest offset submitted for execution per topic-partition */
  def eventLoopLatestOffsets = eventLoopState.latestOffsets
}

case class RecordConsumerTopology(group: Group, subscription: ConsumerSubscription)

case class RecordConsumerConfig(bootstrapServers: String,
                                group: Group,
                                initialSubscription: ConsumerSubscription,
                                retryConfig: Option[RetryConfig] = None,
                                clientId: String = RecordConsumerConfig.makeClientId,
                                eventLoopConfig: EventLoopConfig = EventLoopConfig.Default,
                                offsetReset: OffsetReset = OffsetReset.Latest,
                                extraProperties: Map[String, String] = Map.empty,
                                userProvidedListener: RebalanceListener[Any] = RebalanceListener.Empty,
                                initialOffsetsSeek: InitialOffsetsSeek = InitialOffsetsSeek.default
                               ) extends CommonGreyhoundConfig {

  override def kafkaProps: Map[String, String] = extraProperties
}

object RecordConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}

case class ResubscribeTimeout(resubscribeTimeout: duration.Duration, subscription: ConsumerSubscription) extends RuntimeException(s"Resubscribe timeout (${resubscribeTimeout.getSeconds} s) for $subscription")

abstract class ConsumerConfigFailedValidation(val msg: String) extends RuntimeException(msg)

object ConsumerConfigFailedValidation {

  case object InvalidRetryConfigForPatternSubscription extends ConsumerConfigFailedValidation("A consumer with a pattern subscription cannot be created with a custom retry policy. Use ZRetryConfig.retryForPattern(..)")

}
