package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Dispatcher.Record
import com.wixpress.dst.greyhound.core.consumer.SubmitResult._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import zio._

trait Dispatcher[-R] {
  def submit(record: Record): URIO[R, SubmitResult]
  def resumeablePartitions(paused: Set[TopicPartition]): URIO[R, Set[TopicPartition]]
}

object Dispatcher {
  type Record = ConsumerRecord[Chunk[Byte], Chunk[Byte]]

  def make[R](handle: Record => URIO[R, Unit],
              lowWatermark: Int,
              highWatermark: Int): URManaged[GreyhoundMetrics, Dispatcher[R with GreyhoundMetrics]] =
    Ref.make(Map.empty[TopicPartition, DispatcherWorker]).toManaged { ref =>
      ref.get.flatMap { workers =>
        ZIO.foreach_(workers) {
          case (partition, worker) =>
            Metrics.report(StoppingWorker(partition)) *>
              worker.shutdown
        }
      }
    }.map { ref =>
      new Dispatcher[R with GreyhoundMetrics] {
        override def submit(record: Record): URIO[R with GreyhoundMetrics, SubmitResult] =
          for {
            _ <- Metrics.report(SubmittingRecord(record))
            partition = TopicPartition(record)
            worker <- workerFor(partition)
            promise <- Promise.make[Nothing, Unit]
            submitted <- worker.submit(DispatcherTask(record, promise.succeed(()).unit))
          } yield if (submitted) Submitted(promise.await) else Rejected

        override def resumeablePartitions(paused: Set[TopicPartition]): UIO[Set[TopicPartition]] =
          ref.get.flatMap { workers =>
            ZIO.foldLeft(paused)(Set.empty[TopicPartition]) { (acc, partition) =>
              workers.get(partition) match {
                case Some(worker) =>
                  worker.pendingTasks.map { tasks =>
                    if (tasks <= lowWatermark) acc + partition
                    else acc
                  }

                case None => ZIO.succeed(acc)
              }
            }
          }

        /**
          * This implementation is not fiber-safe. Since the worker is used per partition,
          * and all operations performed on a single partition are linearizable this is fine.
          */
        private def workerFor(partition: TopicPartition) =
          ref.get.flatMap { workers =>
            workers.get(partition) match {
              case Some(worker) => ZIO.succeed(worker)
              case None => for {
                _ <- Metrics.report(StartingWorker(partition))
                worker <- DispatcherWorker.make(handleWithMetrics, highWatermark)
                _ <- ref.update(_ + (partition -> worker))
              } yield worker
            }
          }

        private def handleWithMetrics(record: Record) =
          Metrics.report(HandlingRecord(record)) *> handle(record)
      }
    }

}

sealed trait SubmitResult

object SubmitResult {
  case class Submitted(awaitCompletion: UIO[Unit]) extends SubmitResult
  case object Rejected extends SubmitResult
}

case class DispatcherTask(record: Record, complete: UIO[Unit])

trait DispatcherWorker {
  def submit(task: DispatcherTask): UIO[Boolean]
  def pendingTasks: UIO[Int]
  def shutdown: UIO[Unit]
}

object DispatcherWorker {
  def make[R](handle: Record => URIO[R, Unit],
              capacity: Int): URIO[R, DispatcherWorker] = for {
    queue <- Queue.dropping[DispatcherTask](capacity)
    fiber <- queue.take.flatMap {
      case DispatcherTask(record, complete) =>
        handle(record) *> complete
    }.forever.fork.interruptible
  } yield new DispatcherWorker {
    override def submit(task: DispatcherTask): UIO[Boolean] = queue.offer(task)
    override def pendingTasks: UIO[Int] = queue.size
    override def shutdown: UIO[Unit] = fiber.interrupt.unit
  }
}

sealed trait DispatcherMetric extends GreyhoundMetric
case class StartingWorker(partition: TopicPartition) extends DispatcherMetric
case class StoppingWorker(partition: TopicPartition) extends DispatcherMetric
case class SubmittingRecord[K, V](record: ConsumerRecord[K, V]) extends DispatcherMetric
case class HandlingRecord[K, V](record: ConsumerRecord[K, V]) extends DispatcherMetric
