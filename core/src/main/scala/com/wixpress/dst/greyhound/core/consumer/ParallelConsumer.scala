package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.{Group, Topic}
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.ParallelConsumer.Env
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import zio.{UIO, URIO, ZIO, ZManaged}
import zio.blocking.Blocking
import zio.clock.Clock

trait ParallelConsumer[-R] extends Resource[R] {
  def state: URIO[R with Env, ParallelConsumerExposedState]

  def topology: URIO[R with Env, ParallelConsumerTopology]
}

object ParallelConsumer {
  type Env = GreyhoundMetrics with Blocking with Clock

  /**
   * Creates a parallel consumer, that when used will start consuming messages
   * from Kafka and invoke the appropriate handlers. Handling is concurrent between
   * partitions; order is guaranteed to be maintained within the same partition.
   */
  def make[R](config: ParallelConsumerConfig,
              handlers: Map[Group, Handler[R]]): ZManaged[R with Env, Throwable, ParallelConsumer[R with Env]] =
    ZManaged.foreachPar(handlers) {
      case (group, handler) => for {
        consumer <- Consumer.make(ConsumerConfig(config.bootstrapServers, group, config.clientId))
        eventLoop <- EventLoop.make(group, ReportingConsumer(config.clientId, group, consumer), handler, config.eventLoopConfig)
      } yield (consumer, eventLoop, group)
    }.map { consumers: Seq[(Consumer[Blocking], EventLoop[R with GreyhoundMetrics with Clock], Group)] =>
      new ParallelConsumer[R with Env] {
        override def pause: URIO[R with Env, Unit] =
          ZIO.foreach(consumers)(_._2.pause).unit

        override def resume: URIO[R with Env, Unit] =
          ZIO.foreach(consumers)(_._2.resume).unit

        override def isAlive: URIO[R with Env, Boolean] =
          ZIO.foreach(consumers)(_._2.isAlive).map(_.forall(_ == true))

        override def state: URIO[R with Env, ParallelConsumerExposedState] =
          ZIO.foreach(consumers) { case (_, eventLoop, group) => eventLoop.state.map(state => (group, state)) }.map(_.toMap)
            .map(ParallelConsumerExposedState.apply)

        override def topology: URIO[R with Env with Env, ParallelConsumerTopology] =
          UIO(handlers.mapValues(_.topics)).map(ParallelConsumerTopology.apply)
      }
    }

  def make[R](bootstrapServers: Set[String],
              handlers: (Group, Handler[R])*): ZManaged[R with Env, Throwable, Resource[R with Env]] =
    make(ParallelConsumerConfig(bootstrapServers), handlers.toMap)
}

case class ParallelConsumerExposedState(dispatcherStates: Map[Group, DispatcherExposedState])

case class ParallelConsumerTopology(subscriptions: Map[Group, Set[Topic]])

case class ParallelConsumerConfig(bootstrapServers: Set[String],
                                  clientId: String = ParallelConsumerConfig.DefaultClientId,
                                  eventLoopConfig: EventLoopConfig = EventLoopConfig.Default)

object ParallelConsumerConfig {
  val DefaultClientId = "greyhound-consumers"
}
