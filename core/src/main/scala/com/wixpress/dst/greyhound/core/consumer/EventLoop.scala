package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.{Offset, Topic}
import zio._
import zio.clock.Clock
import zio.duration._

import scala.collection.JavaConverters._

trait EventLoop {
  def pause: UIO[Unit]
  def resume: UIO[Unit]
}

object EventLoop {
  type Handler[R] = RecordHandler[R, Nothing, Chunk[Byte], Chunk[Byte]]

  def make[R](consumer: Consumer[R],
              offsets: Offsets,
              handler: Handler[R],
              partitionsState: PartitionsState = EmptyPartitionsState,
              config: EventLoopConfig = EventLoopConfig.Default): RManaged[R with Clock with GreyhoundMetrics, EventLoop] = {
    val reportingConsumer = ReportingConsumer(consumer)
    val startEventLoop = for {
      _ <- Metrics.report(StartingEventLoop)
      shuttingDown <- Ref.make(false)
      subscriber <- Subscriber.make(reportingConsumer, handler.topics)
      fiber <- run(reportingConsumer, partitionsState, handler, offsets, shuttingDown, config, Map.empty).fork
      _ <- subscriber.waitForPartitionsToBeAssigned
    } yield (shuttingDown, fiber)

    startEventLoop.toManaged {
      case (shuttingDown, fiber) =>
        (for {
          _ <- Metrics.report(StoppingEventLoop)
          _ <- shuttingDown.set(true)
          polledOffsets <- fiber.join
          drained <- offsets.waitFor(polledOffsets).timeout(config.drainTimeout)
          _ <- ZIO.when(drained.isEmpty)(Metrics.report(EventLoopTimeoutExceeded(polledOffsets)))
          _ <- commitOffsets(reportingConsumer, offsets)
        } yield ()).orDie
    }.as {
      new EventLoop {
        override def pause: UIO[Unit] = partitionsState.pause
        override def resume: UIO[Unit] = partitionsState.resume
      }
    }
  }

  private def run[R1, R2](consumer: Consumer[R1],
                          partitionsState: PartitionsState,
                          handler: Handler[R2],
                          offsets: Offsets,
                          shuttingDown: Ref[Boolean],
                          config: EventLoopConfig,
                          polledOffsets: Map[TopicPartition, Offset]): RIO[R1 with R2, Map[TopicPartition, Offset]] =
    resumePartitions(consumer, partitionsState) *>
      pollAndHandle(consumer, handler, config).flatMap { newOffsets =>
        val updatedPolledOffsets = Offsets.merge(polledOffsets, newOffsets)
        pausePartitions(consumer, partitionsState) *>
          commitOffsets(consumer, offsets) *>
          shuttingDown.get.flatMap {
            case false => run(consumer, partitionsState, handler, offsets, shuttingDown, config, updatedPolledOffsets)
            case true => ZIO.succeed(updatedPolledOffsets)
          }
      }

  private def resumePartitions[R](consumer: Consumer[R], partitionsState: PartitionsState) =
    partitionsState.partitionsToResume.flatMap(consumer.resume)

  // TODO how to handle failures?
  private def pausePartitions[R](consumer: Consumer[R], partitionsState: PartitionsState) =
    partitionsState.partitionsToPause.flatMap { partitionsToPause =>
      consumer.pause(partitionsToPause.keySet) *>
        ZIO.foreach_(partitionsToPause) {
          case (partition, offset) =>
            consumer.seek(partition, offset)
        }
    }

  private def pollAndHandle[R1, R2](consumer: Consumer[R1],
                                    handler: Handler[R2],
                                    config: EventLoopConfig) =
    consumer.poll(config.pollTimeout).flatMap { records =>
      ZIO.foldLeft(records.asScala)(Map.empty[TopicPartition, Offset]) { (acc, kafkaRecord) =>
        val record = ConsumerRecord(kafkaRecord)
        val offsets = acc + (TopicPartition(record) -> record.offset)
        handler.handle(record).as(offsets)
      }
    }

  private def commitOffsets[R](consumer: Consumer[R], offsets: Offsets) =
    offsets.committable.flatMap(consumer.commit)
}

case class EventLoopConfig(pollTimeout: Duration,
                           drainTimeout: Duration)

object EventLoopConfig {
  val Default = EventLoopConfig(
    pollTimeout = 100.millis,
    drainTimeout = 30.seconds)
}

trait Subscriber {
  def waitForPartitionsToBeAssigned: UIO[Unit]
}

object Subscriber {
  def make[R](consumer: Consumer[R], topics: Set[Topic]): RIO[R, Subscriber] =
    for {
      ready <- Promise.make[Nothing, Unit]
      _ <- consumer.subscribe(
        topics = topics,
        onPartitionsAssigned = { _ =>
          ready.succeed(()).unit
        })
    } yield new Subscriber {
      override def waitForPartitionsToBeAssigned: UIO[Unit] =
        ready.await
    }
}

sealed trait EventLoopMetric extends GreyhoundMetric
case object StartingEventLoop extends EventLoopMetric
case object StoppingEventLoop extends EventLoopMetric
case class EventLoopTimeoutExceeded(offsets: Map[TopicPartition, Offset]) extends EventLoopMetric
