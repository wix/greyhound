package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Consumers.{Handler, OffsetsMap}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.{Offset, Record, TopicName}
import org.apache.kafka.common.TopicPartition
import zio._
import zio.blocking.Blocking
import zio.duration._

import scala.collection.JavaConverters._

trait EventLoop {
  def pause(partitions: Set[TopicPartition]): RIO[Blocking, Unit]
  def resume(partitions: Set[TopicPartition]): RIO[Blocking, Unit]
}

object EventLoop {
  private val pollTimeout = 100.millis

  def make[R](consumer: Consumer,
              offsets: OffsetsMap,
              handler: Handler[R]): RManaged[R with Blocking with GreyhoundMetrics, EventLoop] =
    for {
      _ <- subscribe(consumer, handler.topics).toManaged_
      _ <- run(consumer, handler, offsets).forever.toManaged_.fork
    } yield new EventLoop {
      override def pause(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
        consumer.pause(partitions)

      override def resume(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
        consumer.resume(partitions)
    }

  private def subscribe(consumer: Consumer, topics: Set[TopicName]) =
    Metrics.report(Subscribing(topics)) *> consumer.subscribe(topics)

  private def run[R](consumer: Consumer, handler: Handler[R], offsets: OffsetsMap) =
    pollAndHandle(consumer, handler) *> commitOffsets(consumer, offsets)

  private def pollAndHandle[R](consumer: Consumer, handler: Handler[R]) =
    consumer.poll(pollTimeout).flatMap { records =>
      ZIO.foreach_(records.asScala) { record =>
        handler.handle(Record(record))
      }
    }

  private def commitOffsets(consumer: Consumer, offsets: OffsetsMap) =
    offsets.modify(current => (current, Map.empty)).flatMap { current =>
      ZIO.when(current.nonEmpty) {
        Metrics.report(CommittingOffsets(current)) *>
          consumer.commit(current)
      }
    }
}

sealed trait EventLoopMetric extends GreyhoundMetric
case class Subscribing(topics: Set[TopicName]) extends EventLoopMetric
case class CommittingOffsets(offsets: Map[TopicPartition, Offset]) extends EventLoopMetric
