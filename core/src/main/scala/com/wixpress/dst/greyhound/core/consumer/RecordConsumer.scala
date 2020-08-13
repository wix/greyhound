package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.consumer.RecordConsumerMetric.UncaughtHandlerError
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerSubscription, RecordHandler, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper.{patternRetryTopic, retryPattern}
import com.wixpress.dst.greyhound.core.consumer.retry._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRetryPolicy, ReportingProducer}
import zio._
import zio.duration._

import scala.util.Random

trait RecordConsumer[-R] extends Resource[R] {
  def group: Group

  def clientId: ClientId

  def state: UIO[RecordConsumerExposedState]

  def topology: UIO[RecordConsumerTopology]

  def resubscribe[R1](subscription: ConsumerSubscription, listener: RebalanceListener[R1] = RebalanceListener.Empty): RIO[Env with R1, AssignedPartitions]

  def setBlockingState(command: BlockingStateCommand): RIO[Env, Unit]

}

object RecordConsumer {
  type Env = ZEnv with GreyhoundMetrics
  type AssignedPartitions = Set[TopicPartition]

  /**
   * Creates a RecordConsumer, that when used will start consuming messages
   * from Kafka and invoke the appropriate handlers. Handling is concurrent between
   * partitions; order is guaranteed to be maintained within the same partition.
   */
  def make[R, E](config: RecordConsumerConfig, handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]]): ZManaged[R with Env, Throwable, RecordConsumer[R with Env]] =
    for {
      consumerSubscriptionRef <- Ref.make[ConsumerSubscription](config.initialSubscription).toManaged_
      nonBlockingRetryHelper = NonBlockingRetryHelper(config.group, config.retryConfig)
      consumer <- Consumer.make(
        ConsumerConfig(config.bootstrapServers, config.group, config.clientId, config.offsetReset, config.extraProperties))
      (initialSubscription, topicsToCreate) = config.retryConfig.fold((config.initialSubscription, Set.empty[Topic]))(policy =>
        maybeAddRetryTopics(config, nonBlockingRetryHelper))
      _ <- AdminClient.make(AdminClientConfig(config.bootstrapServers)).use(client =>
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

      override def state: UIO[RecordConsumerExposedState] = for {
        dispatcherState <-  eventLoop.state
        blockingStateMap <- blockingState.get
      } yield RecordConsumerExposedState(dispatcherState, config.clientId, blockingStateMap)

      override def topology: UIO[RecordConsumerTopology] =
        consumerSubscriptionRef.get.map(subscription => RecordConsumerTopology(subscription))

      override def group: Group = config.group

      override def resubscribe[R1](subscription: ConsumerSubscription, listener: RebalanceListener[R1]): RIO[Env with R1, AssignedPartitions] =
        for {
          assigned <- Ref.make[AssignedPartitions](Set.empty)
          promise <- Promise.make[Nothing, AssignedPartitions]
          rebalanceListener = listener *> new RebalanceListener[R1] {
            override def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[R1, Any] =
              ZIO.unit

            //todo: we need to call EventLoop's listener here! otherwise we don't stop fibers on resubscribe

            override def onPartitionsAssigned(partitions: Set[TopicPartition]): URIO[R1, Any] = for {
              allAssigned <- assigned.updateAndGet(_ => partitions)
              _ <- consumerSubscriptionRef.set(subscription)
              _ <- promise.succeed(allAssigned)
            } yield ()
          }

          _ <- subscribe[R1](subscription, rebalanceListener)(consumer)
          result <- promise.await
        } yield result

      override def clientId: ClientId = config.clientId
    }

  private def maybeAddRetryTopics[E, R](config: RecordConsumerConfig, helper: NonBlockingRetryHelper): (ConsumerSubscription, Set[String]) = {
    config.initialSubscription match {
      case Topics(topics) =>
        val retryTopics = topics.flatMap(helper.retryTopicsFor)
        (Topics(topics ++ retryTopics), retryTopics)
      case TopicPattern(pattern, _) => (TopicPattern(Pattern.compile(s"${pattern.pattern}|${retryPattern(config.group)}")),
        (0 until helper.retrySteps).map(step => patternRetryTopic(config.group, step)).toSet)
    }
  }

  private def addRetriesToHandler[R, E](config: RecordConsumerConfig,
                                        handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]],
                                        blockingState: Ref[Map[BlockingTarget, BlockingState]],
                                        nonBlockingRetryHelper: NonBlockingRetryHelper) =
    config.retryConfig match {
      case Some(retryConfig) =>
        Producer.makeR[R](ProducerConfig(config.bootstrapServers, retryPolicy = ProducerRetryPolicy(Int.MaxValue, 3.seconds))).map(producer =>
          ReportingProducer(producer))
          .map(producer => RetryRecordHandler.withRetries(handler, retryConfig, producer, config.initialSubscription, blockingState, nonBlockingRetryHelper))
      case None =>
        ZManaged.succeed(handler.withErrorHandler((e, record) =>
          report(UncaughtHandlerError(e, record.topic, record.partition, record.offset, config.group, config.clientId))))
    }
}

sealed trait RecordConsumerMetric extends GreyhoundMetric {
  def group: Group

  def clientId: ClientId
}

object RecordConsumerMetric {

  case class UncaughtHandlerError[E](error: E, topic: Topic, partition: Partition, offset: Offset, group: Group, clientId: ClientId) extends RecordConsumerMetric

}

case class RecordConsumerExposedState(dispatcherState: DispatcherExposedState, consumerId: String, blockingState: Map[BlockingTarget, BlockingState]) {
  def topics = dispatcherState.topics
}

case class RecordConsumerTopology(subscription: ConsumerSubscription)

case class RecordConsumerConfig(bootstrapServers: String,
                                group: Group,
                                initialSubscription: ConsumerSubscription,
                                retryConfig: Option[RetryConfig] = None,
                                clientId: String = RecordConsumerConfig.makeClientId,
                                eventLoopConfig: EventLoopConfig = EventLoopConfig.Default,
                                offsetReset: OffsetReset = OffsetReset.Latest,
                                extraProperties: Map[String, String] = Map.empty)

object RecordConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}
