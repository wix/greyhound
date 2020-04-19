package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{Group, Topic}
import zio.blocking.Blocking
import zio.clock.Clock
import zio._

import scala.util.Random

trait RecordConsumer[-R] extends Resource[R] {
  def group: Group

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
  def make[R](config: RecordConsumerConfig, handler: Handler[R]): ZManaged[R with Env, Throwable, RecordConsumer[R with Env]] =
    (for {
      consumer <- Consumer.make(ConsumerConfig(config.bootstrapServers, config.group, config.clientId, config.offsetReset, config.extraProperties))
      eventLoop <- EventLoop.make(config.group, ReportingConsumer(config.clientId, config.group, consumer), handler, config.eventLoopConfig)
    } yield (consumer, eventLoop))
      .map { case (consumer, eventLoop) =>
        new RecordConsumer[R with Env] {
          override def pause: URIO[R with Env, Unit] =
            eventLoop.pause

          override def resume: URIO[R with Env, Unit] =
            eventLoop.resume

          override def isAlive: URIO[R with Env, Boolean] =
            eventLoop.isAlive

          override def state[R1]: URIO[Env with R with R1, RecordConsumerExposedState] =
            eventLoop.state.map(RecordConsumerExposedState.apply)

          override def topology[R1]: URIO[Env with R with R1, RecordConsumerTopology] =
            UIO(RecordConsumerTopology(handler.topics))

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
        }
      }
}

case class RecordConsumerExposedState(dispatcherState: DispatcherExposedState) {
  def topics = dispatcherState.topics
}

case class RecordConsumerTopology(subscriptions: Set[Topic])

case class RecordConsumerConfig(bootstrapServers: Set[String],
                                group: Group,
                                clientId: String = RecordConsumerConfig.makeClientId,
                                eventLoopConfig: EventLoopConfig = EventLoopConfig.Default,
                                offsetReset: OffsetReset = OffsetReset.Latest,
                                extraProperties: Map[String, String] = Map.empty)

object RecordConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}
