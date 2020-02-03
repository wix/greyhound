package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import zio.ZManaged
import zio.blocking.Blocking
import zio.clock.Clock

object ParallelConsumer {
  type Env = GreyhoundMetrics with Blocking with Clock

  def make[R](config: ParallelConsumerConfig,
              handlers: Map[Group, Handler[R]]): ZManaged[R with Env, Throwable, Map[Group, EventLoop[R with Env]]] =
    ZManaged.foreachPar(handlers) {
      case (group, handler) => for {
        consumer <- Consumer.make(ConsumerConfig(config.bootstrapServers, group, config.clientId))
        eventLoop <- EventLoop.make(ReportingConsumer(group, consumer), handler, config.eventLoopConfig)
      } yield group -> eventLoop
    }.map(_.toMap)

  def make[R](bootstrapServers: Set[String],
              handlers: (Group, Handler[R])*): ZManaged[R with Env, Throwable, Map[Group, EventLoop[R with Env]]] =
    make(ParallelConsumerConfig(bootstrapServers), handlers.toMap)

}

case class ParallelConsumerConfig(bootstrapServers: Set[String],
                                  clientId: String = ParallelConsumerConfig.DefaultClientId,
                                  eventLoopConfig: EventLoopConfig = EventLoopConfig.Default)

object ParallelConsumerConfig {
  val DefaultClientId = "greyhound-consumers"
}
