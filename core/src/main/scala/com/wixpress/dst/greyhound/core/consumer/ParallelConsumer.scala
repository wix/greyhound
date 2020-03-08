package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import zio.{URIO, ZManaged}
import zio.blocking.Blocking
import zio.clock.Clock

trait ParallelConsumer[-R] extends Resource[R]

object ParallelConsumer {
  type Env = GreyhoundMetrics with Blocking with Clock

  /**
    * Creates parallel consumer, that when used will start consuming messages
    * from Kafka and invoke the appropriate handlers. Handling is concurrent between
    * partitions, order is guaranteed to be maintained within the same partition.
    */
  def make[R](config: ParallelConsumerConfig,
              handlers: Map[Group, Handler[R]]): ZManaged[R with Env, Throwable, ParallelConsumer[R with Env]] =
    ZManaged.foreachPar(handlers) {
      case (group, handler) => for {
        consumer <- Consumer.make(ConsumerConfig(config.bootstrapServers, group, config.clientId))
        eventLoop <- EventLoop.make(group, ReportingConsumer(config.clientId, group, consumer), handler, config.eventLoopConfig)
      } yield eventLoop
    }.map { eventLoops =>
      val combined = eventLoops.foldLeft[Resource[R with Env]](Resource.Empty)(_ combine _)

      new ParallelConsumer[R with Env] {
        override def pause: URIO[R with Env, Unit] =
          combined.pause

        override def resume: URIO[R with Env, Unit] =
          combined.resume

        override def isAlive: URIO[R with Env, Boolean] =
          combined.isAlive
      }
    }

  def make[R](bootstrapServers: Set[String],
              handlers: (Group, Handler[R])*): ZManaged[R with Env, Throwable, Resource[R with Env]] =
    make(ParallelConsumerConfig(bootstrapServers), handlers.toMap)

}

case class ParallelConsumerConfig(bootstrapServers: Set[String],
                                  clientId: String = ParallelConsumerConfig.DefaultClientId,
                                  eventLoopConfig: EventLoopConfig = EventLoopConfig.Default)

object ParallelConsumerConfig {
  val DefaultClientId = "greyhound-consumers"
}
