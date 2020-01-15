package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import zio._
import zio.duration._

import scala.collection.JavaConverters._

trait EventLoop {
  def pause: UIO[Unit]
  def resume: UIO[Unit]
}

object EventLoop {
  type Handler[R] = RecordHandler[R, Nothing, Chunk[Byte], Chunk[Byte]]

  private val pollTimeout = 100.millis

  def make[R](consumer: Consumer[R],
              offsets: Offsets,
              handler: Handler[R],
              partitionsState: PartitionsState = EmptyPartitionsState): RManaged[R with GreyhoundMetrics, EventLoop] = {
    val consumer1 = ReportingConsumer(consumer)
    for {
      subscriber <- Subscriber.make(consumer1, handler.topics).toManaged_
      _ <- run(consumer1, partitionsState, handler, offsets).forever.toManaged_.fork
      _ <- subscriber.await.toManaged_
    } yield new EventLoop {
      override def pause: UIO[Unit] = partitionsState.pause
      override def resume: UIO[Unit] = partitionsState.resume
    }
  }

  private def run[R1, R2](consumer: Consumer[R1],
                          partitionsState: PartitionsState,
                          handler: Handler[R2],
                          offsets: Offsets) =
    resumePartitions(consumer, partitionsState) *>
      pollAndHandle(consumer, handler) *>
      pausePartitions(consumer, partitionsState) *>
      commitOffsets(consumer, offsets)

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

  private def pollAndHandle[R1, R2](consumer: Consumer[R1], handler: Handler[R2]) =
    consumer.poll(pollTimeout).flatMap { records =>
      ZIO.foreach_(records.asScala) { record =>
        handler.handle(ConsumerRecord(record))
      }
    }

  private def commitOffsets[R](consumer: Consumer[R], offsets: Offsets) =
    offsets.getAndClear.flatMap(consumer.commit)
}

trait Subscriber {
  def await: UIO[Unit]
}

object Subscriber {
  def make[R](consumer: Consumer[R], topics: Set[Topic]): RIO[R, Subscriber] =
    for {
      ready <- Promise.make[Nothing, Unit]
      partitionsToAssign <- Ref.make(Set.empty[TopicPartition])
      _ <- consumer.partitionsFor(topics).flatMap { partitions =>
        if (partitions.isEmpty) ready.succeed(())
        else partitionsToAssign.set(partitions)
      }
      _ <- consumer.subscribe(
        topics = topics,
        onPartitionsAssigned = { partitions =>
          partitionsToAssign.update(_ diff partitions).flatMap { remaining =>
            ZIO.when(remaining.isEmpty)(ready.succeed(()))
          }
        })
    } yield new Subscriber {
      override def await: UIO[Unit] =
        ready.await
    }
}
