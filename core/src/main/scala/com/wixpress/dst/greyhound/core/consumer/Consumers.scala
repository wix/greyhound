package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{Group, Record}
import org.apache.kafka.common.TopicPartition
import zio._
import zio.blocking.Blocking

// TODO rename ConsumersRunner?
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
          eventLoop <- EventLoop.make[R](consumer, offsets, spec.andThen(updateOffsets(offsets)))
        } yield group -> eventLoop
    }.map(_.toMap)

  private def updateOffsets(offsets: Offsets)(record: Record[Chunk[Byte], Chunk[Byte]]): UIO[Unit] =
    offsets.update(new TopicPartition(record.topic, record.partition), record.offset)
}
