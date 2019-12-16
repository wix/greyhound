package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.ParallelRecordHandler.{Handler, OffsetsMap}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{CommittingOffsets, Metrics, Subscribing}
import com.wixpress.dst.greyhound.core.{Record, TopicName}
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

  def make(consumer: Consumer,
           offsets: OffsetsMap,
           handler: Handler,
           topics: Set[TopicName]): ZManaged[Blocking with GreyhoundMetrics, Throwable, EventLoop] =
    for {
      _ <- subscribe(consumer, topics).toManaged_
      _ <- run(consumer, handler, offsets).forever.toManaged_.fork
    } yield new EventLoop {
      override def pause(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
        consumer.pause(partitions)

      override def resume(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
        consumer.resume(partitions)
    }

  private def subscribe(consumer: Consumer, topics: Set[TopicName]) =
    Metrics.report(Subscribing(topics)) *> consumer.subscribe(topics)

  private def run(consumer: Consumer, handler: Handler, offsets: OffsetsMap) =
    pollAndHandle(consumer, handler) *> commitOffsets(consumer, offsets)

  private def pollAndHandle(consumer: Consumer, handler: Handler) =
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
