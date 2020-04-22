package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRetryPolicy, ReportingProducer}
import com.wixpress.dst.greyhound.core.{ClientId, Group, NonEmptySet, Topic}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.util.Random

trait RecordConsumer[-R] extends Resource[R] {
  def group: Group

  def clientId: ClientId

  def state[R1]: URIO[Env with R with R1, RecordConsumerExposedState]

  def topology[R1]: URIO[Env with R with R1, RecordConsumerTopology]

  def resubscribe[R1](topics: Set[Topic], listener: RebalanceListener[R1] = RebalanceListener.Empty): RIO[Env with R1, AssignedPartitions]

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
      consumer <- Consumer.make(
        ConsumerConfig(config.bootstrapServers, config.group, config.clientId, config.offsetReset, config.extraProperties))
      allInitiallySubscribedTopics = config.retryPolicy.fold(config.initialTopics)(policy => config.initialTopics.flatMap(policy.retryTopicsFor))
      handlerWithRetries <- addRetriesToHandler(config, handler)
      eventLoop <- EventLoop.make(
        group = config.group,
        initialTopics = allInitiallySubscribedTopics,
        consumer = ReportingConsumer(config.clientId, config.group, consumer),
        handler = handlerWithRetries,
        config = config.eventLoopConfig)
    } yield new RecordConsumer[R with Env] {
      override def pause: URIO[R with Env, Unit] =
        eventLoop.pause

      override def resume: URIO[R with Env, Unit] =
        eventLoop.resume

      override def isAlive: URIO[R with Env, Boolean] =
        eventLoop.isAlive

      override def state[R1]: URIO[Env with R with R1, RecordConsumerExposedState] =
        eventLoop.state.map(RecordConsumerExposedState.apply)

      override def topology[R1]: URIO[Env with R with R1, RecordConsumerTopology] =
        UIO(RecordConsumerTopology(allInitiallySubscribedTopics)) //todo: this should be updated on resubscribe

      override def group: Group = config.group

      override def resubscribe[R1](topics: Set[Topic], listener: RebalanceListener[R1]): RIO[Env with R1, AssignedPartitions] =
        for {
          promise <- Promise.make[Nothing, AssignedPartitions]
          _ <- consumer.subscribe[R1](topics, listener *> new RebalanceListener[R1] {
            override def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[R1, Any] =
              ZIO.unit

            override def onPartitionsAssigned(partitions: Set[TopicPartition]): URIO[R1, Any] =
              promise.succeed(partitions)
          })
          result <- promise.await
        } yield result

      override def clientId: ClientId = config.clientId
    }

  private def addRetriesToHandler[R, E, R3](config: RecordConsumerConfig, handler: RecordHandler[R, E, Chunk[Byte], Chunk[Byte]]) =
    config.retryPolicy match {
      case Some(policy) =>
        Producer.make[Clock](ProducerConfig(config.bootstrapServers, retryPolicy = ProducerRetryPolicy(Int.MaxValue, 3.seconds))).map(ReportingProducer(_))
          .map(producer => RetryRecordHandler.withRetries(handler, policy, producer))
      case None =>
        ZManaged.succeed(handler.withErrorHandler((_, _) => ZIO.unit)) //todo: report uncaught handler errors?
    }
}

case class RecordConsumerExposedState(dispatcherState: DispatcherExposedState) {
  def topics = dispatcherState.topics
}

case class RecordConsumerTopology(subscriptions: Set[Topic])

case class RecordConsumerConfig(bootstrapServers: String,
                                group: Group,
                                initialTopics: NonEmptySet[Topic],
                                retryPolicy: Option[RetryPolicy] = None,
                                clientId: String = RecordConsumerConfig.makeClientId,
                                eventLoopConfig: EventLoopConfig = EventLoopConfig.Default,
                                offsetReset: OffsetReset = OffsetReset.Latest,
                                extraProperties: Map[String, String] = Map.empty)

object RecordConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}
