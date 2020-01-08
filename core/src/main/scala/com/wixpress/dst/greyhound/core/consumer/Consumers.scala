package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import zio._
import zio.blocking.Blocking

// TODO rename ConsumersRunner?
// TODO is this needed?
object Consumers {
  private val clientId = "greyhound-consumers"

  type Handler[R] = RecordHandler[R, Nothing, Chunk[Byte], Chunk[Byte]]

  def make[R](bootstrapServers: Set[String], specs: Map[Group, Handler[R]]): RManaged[R with Blocking with GreyhoundMetrics, Map[Group, EventLoop]] =
    ZManaged.foreachPar(specs) {
      case (group, spec) =>
        for {
          // TODO is this the right order for handling offsets?
          offsets <- Offsets.make.toManaged_
          consumer <- Consumer.make(ConsumerConfig(bootstrapServers, group, clientId))
          eventLoop <- EventLoop.make[R](consumer, offsets, spec.andThen(offsets.update))
        } yield group -> eventLoop
    }.map(_.toMap)
}
