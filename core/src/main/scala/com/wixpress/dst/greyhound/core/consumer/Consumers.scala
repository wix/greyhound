package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{GroupName, Offset, Record}
import org.apache.kafka.common.TopicPartition
import zio._
import zio.blocking.Blocking

// TODO rename ConsumersRunner?
object Consumers {
  private val clientId = "greyhound-consumers"

  type Handler[R] = RecordHandler[R, Nothing, Chunk[Byte], Chunk[Byte]]
  type OffsetsMap = Ref[Map[TopicPartition, Offset]]

  def make[R](bootstrapServers: Set[String], specs: Map[GroupName, Handler[R]]): RManaged[R with Blocking with GreyhoundMetrics, Map[GroupName, EventLoop]] =
    ZManaged.foreachPar(specs) {
      case (group, spec) =>
        for {
          // TODO is this the right order for handling offsets?
          offsets <- Ref.make(Map.empty[TopicPartition, Offset]).toManaged_
          consumer <- Consumer.make(ConsumerConfig(bootstrapServers, group, clientId))
          eventLoop <- EventLoop.make[R](consumer, offsets, spec.andThen(updateOffsets(offsets)))
        } yield group -> eventLoop
    }.map(_.toMap)

  // TODO extract and test
  private def updateOffsets(offsets: OffsetsMap)(record: Record[Chunk[Byte], Chunk[Byte]]): UIO[Unit] = {
    val topicPartition = new TopicPartition(record.topic, record.partition)
    offsets.update { map =>
      val offset = map.get(topicPartition) match {
        case Some(existing) => record.offset max existing
        case None => record.offset
      }
      map + (topicPartition -> offset)
    }.unit
  }
}
