package com.wixpress.dst.greyhound.core.consumer

import java.util.regex.Pattern

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import com.wixpress.dst.greyhound.core.consumer.BlockingState.{Blocking => InternalBlocking, IgnoringAll, IgnoringOnce}
import com.wixpress.dst.greyhound.core.consumer.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.consumer.RecordConsumerMetric.UncaughtHandlerError
import com.wixpress.dst.greyhound.core.consumer.NonBlockingRetryPolicy.{patternRetryTopic, retryPattern}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRetryPolicy, ReportingProducer}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.util.Random

trait RecordConsumer[-R] extends Resource[R] {
  def group: Group

  def clientId: ClientId

  def state: UIO[RecordConsumerExposedState]

  def topology: UIO[RecordConsumerTopology]

  def resubscribe[R1](topics: Set[Topic], listener: RebalanceListener[R1] = RebalanceListener.Empty): RIO[Env with R1, AssignedPartitions]

  def setBlockingState[R1](command: BlockingStateCommand): RIO[Env with R1, Unit]

}

object RecordConsumer {
  type Env = GreyhoundMetrics with Blocking with Clock
  type AssignedPartitions = Set[TopicPartition]

  /**
   * Creates a RecordConsumer, that when used will start consuming messages
   * from Kafka and invoke the appropriate handlers. Handling is concurrent between
   * partitions; order is guaranteed to be maintained within the same partition.
   */
  def make[R, E](config: RecordConsumerConfig, handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]]): ZManaged[R with Env, Throwable, RecordConsumer[R with Env]] =
    for {
      consumerSubscriptionRef <- Ref.make[consumer.ConsumerSubscription](config.initialSubscription).toManaged_
      nonBlockingRetryPolicy = NonBlockingRetryPolicy(config.group, config.retryConfig)
      consumer <- Consumer.make(
        ConsumerConfig(config.bootstrapServers, config.group, config.clientId, config.offsetReset, config.extraProperties))
      (initialSubscription, topicsToCreate) = config.retryConfig.fold((config.initialSubscription, Set.empty[Topic]))(policy =>
        maybeAddRetryTopics(config, nonBlockingRetryPolicy))
      _ <- AdminClient.make(AdminClientConfig(config.bootstrapServers)).use(client =>
        client.createTopics(topicsToCreate.map(topic => TopicConfig(topic, partitions = 1, replicationFactor = 1, cleanupPolicy = CleanupPolicy.Delete(86400000L))))
      ).toManaged_
      blockingState <- Ref.make[Map[BlockingTarget, BlockingState]](Map.empty).toManaged_
      handlerWithRetries <- addRetriesToHandler(config, handler, blockingState, nonBlockingRetryPolicy)
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

      override def setBlockingState[R1](command: BlockingStateCommand): RIO[Env with R1, Unit] = {
        command match {
          case IgnoreOnceFor(topicPartition: TopicPartition)  => blockingState.update(_.updated(TopicPartitionTarget(topicPartition), IgnoringOnce))
          case IgnoreAllFor(topicPartition: TopicPartition)   => blockingState.update(_.updated(TopicPartitionTarget(topicPartition), IgnoringAll))
          case BlockErrorsFor(topicPartition: TopicPartition) => blockingState.update(_.updated(TopicPartitionTarget(topicPartition), InternalBlocking))
          case IgnoreAll(topic: Topic)                        => blockingState.update(_.updated(TopicTarget(topic), IgnoringAll))
          case BlockErrors(topic: Topic)                      => blockingState.update(_.updated(TopicTarget(topic), InternalBlocking))
          case _                                              => ZIO.fail(new RuntimeException(s"unfamiliar BlockingStateCommand: $command"))
        }
      }

      override def state: UIO[RecordConsumerExposedState] =
        eventLoop.state.map(state => RecordConsumerExposedState(state, config.clientId))

      override def topology: UIO[RecordConsumerTopology] =
        consumerSubscriptionRef.get.map(subscription => RecordConsumerTopology(subscription))

      override def group: Group = config.group

      override def resubscribe[R1](topics: Set[Topic], listener: RebalanceListener[R1]): RIO[Env with R1, AssignedPartitions] =
        for {
          assigned <- Ref.make[AssignedPartitions](Set.empty)
          promise <- Promise.make[Nothing, AssignedPartitions]
          _ <- consumer.subscribe[R1](topics, listener *> new RebalanceListener[R1] {
            override def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[R1, Any] =
              ZIO.unit

            //todo: we need to call EventLoop's listener here! otherwise we don't stop fibers on resubscribe

            override def onPartitionsAssigned(partitions: Set[TopicPartition]): URIO[R1, Any] = for {
              allAssigned <- assigned.updateAndGet(_ ++ partitions)
              _ <- consumerSubscriptionRef.set(ConsumerSubscription.Topics(topics))
              _ <- ZIO.when(allAssigned.map(_.topic) == topics)(
                promise.succeed(allAssigned)
              )
            } yield ()
          })
          result <- promise.await
        } yield result

      override def clientId: ClientId = config.clientId
    }

  private def maybeAddRetryTopics[E, R](config: RecordConsumerConfig, policy: NonBlockingRetryPolicy): (ConsumerSubscription, Set[String]) = {
      config.initialSubscription match {
        case Topics(topics) =>
          val retryTopics = topics.flatMap(policy.retryTopicsFor)
          // TODO: topics ++ seems meaningless here
          (Topics(topics ++ retryTopics), retryTopics)
        case TopicPattern(pattern, _) => (TopicPattern(Pattern.compile(s"${pattern.pattern}|${retryPattern(config.group)}")),
          (0 until policy.retrySteps).map(step => patternRetryTopic(config.group, step)).toSet)
      }
  }

  private def addRetriesToHandler[R, E](config: RecordConsumerConfig,
                                        handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]],
                                        blockingState: Ref[Map[BlockingTarget, BlockingState]],
                                        nonBlockingRetryPolicy: NonBlockingRetryPolicy) =
    config.retryConfig match {
      case Some(retryConfig) =>
        Producer.make(ProducerConfig(config.bootstrapServers, retryPolicy = ProducerRetryPolicy(Int.MaxValue, 3.seconds))).map(producer =>
          ReportingProducer(producer))
          .map(producer => RetryRecordHandler.withRetries(handler, retryConfig, producer, config.initialSubscription, blockingState, nonBlockingRetryPolicy))
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

case class RecordConsumerExposedState(dispatcherState: DispatcherExposedState, consumerId: String) {
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

sealed trait ConsumerSubscription

object ConsumerSubscription {

  case class TopicPattern(p: Pattern, discoveredTopics: Set[Topic] = Set.empty) extends ConsumerSubscription

  case class Topics(topics: NonEmptySet[Topic]) extends ConsumerSubscription

}

sealed trait BlockingStateCommand

case class IgnoreOnceFor(topicPartition: TopicPartition) extends BlockingStateCommand
case class IgnoreAllFor(topicPartition: TopicPartition) extends BlockingStateCommand
case class BlockErrorsFor(topicPartition: TopicPartition) extends BlockingStateCommand
case class IgnoreAll(topic: Topic) extends BlockingStateCommand
case class BlockErrors(topic: Topic) extends BlockingStateCommand
