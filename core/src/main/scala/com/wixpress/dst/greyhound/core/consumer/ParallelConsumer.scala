package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.ParallelConsumer.{AssignedPartitions, Env}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{Group, Topic}
import zio.blocking.Blocking
import zio.clock.Clock
import zio._

import scala.util.Random

trait ParallelConsumer[-R] extends Resource[R] {
  def group: Group

  def state[R1]: URIO[Env with R with R1, ParallelConsumerExposedState]

  def topology[R1]: URIO[Env with R with R1, ParallelConsumerTopology]

  def resubscribe[R1](topics: Set[Topic], listener: RebalanceListener[R1] = RebalanceListener.Empty): RIO[Env with R1, AssignedPartitions]

}

object ParallelConsumer {
  type Env = GreyhoundMetrics with Blocking with Clock
  type AssignedPartitions = Set[TopicPartition]

  /**
   * Creates a parallel consumer, that when used will start consuming messages
   * from Kafka and invoke the appropriate handlers. Handling is concurrent between
   * partitions; order is guaranteed to be maintained within the same partition.
   */
  def make[R](config: ParallelConsumerConfig, handler: Handler[R]): ZManaged[R with Env, Throwable, ParallelConsumer[R with Env]] =
    (for {
      consumer <- Consumer.make(ConsumerConfig(config.bootstrapServers, config.group, config.clientId, config.offsetReset, config.extraProperties))
      eventLoop <- EventLoop.make(config.group, ReportingConsumer(config.clientId, config.group, consumer), handler, config.eventLoopConfig)
    } yield (consumer, eventLoop))
      .map { case (consumer, eventLoop) =>
        new ParallelConsumer[R with Env] {
          override def pause: URIO[R with Env, Unit] =
            eventLoop.pause

          override def resume: URIO[R with Env, Unit] =
            eventLoop.resume

          override def isAlive: URIO[R with Env, Boolean] =
            eventLoop.isAlive

          override def state[R1]: URIO[Env with R with R1, ParallelConsumerExposedState] =
            eventLoop.state.map(ParallelConsumerExposedState.apply)

          override def topology[R1]: URIO[Env with R with R1, ParallelConsumerTopology] =
            UIO(ParallelConsumerTopology(handler.topics))

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

case class ParallelConsumerExposedState(dispatcherState: DispatcherExposedState) {
  def topics = dispatcherState.topics
}

case class ParallelConsumerTopology(subscriptions: Set[Topic])

case class ParallelConsumerConfig(bootstrapServers: Set[String],
                                  group: Group,
                                  clientId: String = ParallelConsumerConfig.makeClientId,
                                  eventLoopConfig: EventLoopConfig = EventLoopConfig.Default,
                                  offsetReset: OffsetReset = OffsetReset.Latest,
                                  extraProperties: Map[String, String] = Map.empty)

object ParallelConsumerConfig {
  def makeClientId = s"greyhound-consumer-${Random.alphanumeric.take(5).mkString}"
}
