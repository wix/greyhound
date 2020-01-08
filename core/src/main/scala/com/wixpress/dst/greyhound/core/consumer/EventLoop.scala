package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.{Offset, Topic}
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
  type Handler[R] = RecordHandler[R, Nothing, Chunk[Byte], Chunk[Byte]]

  private val pollTimeout = 100.millis

  def make[R](consumer: Consumer,
              offsets: Offsets,
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

  private def subscribe(consumer: Consumer, topics: Set[Topic]) =
    Metrics.report(Subscribing(topics)) *> consumer.subscribe(topics)

  private def run[R](consumer: Consumer, handler: Handler[R], offsets: Offsets) =
    pollAndHandle(consumer, handler) *> commitOffsets(consumer, offsets)

  private def pollAndHandle[R](consumer: Consumer, handler: Handler[R]) =
    consumer.poll(pollTimeout).flatMap { records =>
      ZIO.foreach_(records.asScala) { record =>
        handler.handle(ConsumerRecord(record))
      }
    }

  private def commitOffsets(consumer: Consumer, offsets: Offsets) =
    offsets.getAndClear.flatMap { current =>
      ZIO.when(current.nonEmpty) {
        Metrics.report(CommittingOffsets(current)) *>
          consumer.commit(current)
      }
    }
}

sealed trait EventLoopMetric extends GreyhoundMetric
case class Subscribing(topics: Set[Topic]) extends EventLoopMetric
case class CommittingOffsets(offsets: Map[TopicPartition, Offset]) extends EventLoopMetric
