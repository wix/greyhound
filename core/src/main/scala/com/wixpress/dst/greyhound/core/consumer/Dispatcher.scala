package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.{Group, Topic}
import com.wixpress.dst.greyhound.core.consumer.Dispatcher.Record
import com.wixpress.dst.greyhound.core.consumer.DispatcherMetric._
import com.wixpress.dst.greyhound.core.consumer.SubmitResult._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import zio._
import zio.clock.Clock
import zio.duration.Duration

trait Dispatcher[-R] {
  def submit(record: Record): URIO[R, SubmitResult]

  def resumeablePartitions(paused: Set[TopicPartition]): URIO[R, Set[TopicPartition]]

  def revoke(partitions: Set[TopicPartition]): URIO[R, Unit]

  def pause: URIO[R, Unit]

  def resume: URIO[R, Unit]

  def shutdown: URIO[R, Unit]

  def expose: URIO[R, DispatcherExposedState]

}

object Dispatcher {
  type Record = ConsumerRecord[Chunk[Byte], Chunk[Byte]]

  def make[R](group: Group,
              handle: Record => URIO[R, Any],
              lowWatermark: Int,
              highWatermark: Int): UIO[Dispatcher[R with GreyhoundMetrics with Clock]] =
    for {
      state <- Ref.make[State](State.Running)
      workers <- Ref.make(Map.empty[TopicPartition, Worker])
    } yield new Dispatcher[R with GreyhoundMetrics with Clock] {
      override def submit(record: Record): URIO[R with GreyhoundMetrics with Clock, SubmitResult] =
        for {
          _ <- Metrics.report(SubmittingRecord(record))
          partition = TopicPartition(record)
          worker <- workerFor(partition)
          submitted <- worker.submit(record)
        } yield if (submitted) Submitted else Rejected

      override def resumeablePartitions(paused: Set[TopicPartition]): UIO[Set[TopicPartition]] =
        workers.get.flatMap { workers =>
          ZIO.foldLeft(paused)(Set.empty[TopicPartition]) { (acc, partition) =>
            workers.get(partition) match {
              case Some(worker) =>
                worker.expose.map { tasks =>
                  if (tasks.queuedTasks <= lowWatermark) acc + partition
                  else acc
                }

              case None => ZIO.succeed(acc)
            }
          }
        }

      override def expose: URIO[R with GreyhoundMetrics with Clock, DispatcherExposedState] =
        workers.get.flatMap(workers =>
          ZIO.foreach(workers) { case (tp, worker) => worker.expose.map(pending => (tp, pending)) }.map(_.toMap)
        ).map(DispatcherExposedState.apply)

      override def revoke(partitions: Set[TopicPartition]): URIO[R with GreyhoundMetrics with Clock, Unit] =
        workers.modify { workers =>
          partitions.foldLeft((List.empty[(TopicPartition, Worker)], workers)) {
            case ((revoked, remaining), partition) =>
              remaining.get(partition) match {
                case Some(worker) => ((partition, worker) :: revoked, remaining - partition)
                case None => (revoked, remaining)
              }
          }
        }.flatMap(shutdown)

      override def pause: UIO[Unit] = for {
        resume <- Promise.make[Nothing, Unit]
        _ <- state.updateSome {
          case State.Running =>
            State.Paused(resume)
        }
      } yield ()

      override def resume: UIO[Unit] = state.modify {
        case State.Paused(resume) => (resume.succeed(()).unit, State.Running)
        case state => (ZIO.unit, state)
      }.flatten

      override def shutdown: URIO[R with GreyhoundMetrics with Clock, Unit] =
        state.modify(state => (state, State.ShuttingDown)).flatMap {
          case State.Paused(resume) => resume.succeed(()).unit
          case _ => ZIO.unit
        } *> workers.get.flatMap(shutdown)

      /**
       * This implementation is not fiber-safe. Since the worker is used per partition,
       * and all operations performed on a single partition are linearizable this is fine.
       */
      private def workerFor(partition: TopicPartition) =
        workers.get.flatMap { workers1 =>
          workers1.get(partition) match {
            case Some(worker) => ZIO.succeed(worker)
            case None => for {
              _ <- Metrics.report(StartingWorker(partition))
              worker <- Worker.make(state, handleWithMetrics, highWatermark)
              _ <- workers.update(_ + (partition -> worker))
            } yield worker
          }
        }

      private def handleWithMetrics(record: Record) =
        Metrics.report(HandlingRecord(record, System.currentTimeMillis() - record.pollTime, group)) *>
          handle(record).timed.flatMap {
            case (duration, _) =>
              Metrics.report(RecordHandled(record, group, duration))
          }

      private def shutdown(workers: Iterable[(TopicPartition, Worker)]) =
        ZIO.foreachPar_(workers) {
          case (partition, worker) =>
            Metrics.report(StoppingWorker(partition)) *>
              worker.shutdown
        }
    }

  sealed trait State

  object State {

    case object Running extends State

    case class Paused(resume: Promise[Nothing, Unit]) extends State

    case object ShuttingDown extends State

  }

  case class Task(record: Record, complete: UIO[Unit])

  trait Worker {
    def submit(record: Record): UIO[Boolean]

    def expose: UIO[WorkerExposedState]

    def shutdown: UIO[Unit]
  }

  object Worker {
    def make[R](status: Ref[State],
                handle: Record => URIO[R, Any],
                capacity: Int): URIO[R, Worker] = for {
      queue <- Queue.dropping[Record](capacity)
      internalState <- Ref.make(WorkerInternalState.empty)
      fiber <- loop(status, internalState, handle, queue).interruptible.fork
    } yield new Worker {
      override def submit(record: Record): UIO[Boolean] =
        queue.offer(record)

      override def expose: UIO[WorkerExposedState] =
        (queue.size zip internalState.get)
          .map { case (queued, state) => WorkerExposedState(
            Math.max(0, queued), state.currentExecutionStarted.map(System.currentTimeMillis - _))
          }

      override def shutdown: UIO[Unit] =
        fiber.interrupt.unit
    }

    private def loop[R](state: Ref[State],
                        internalState: Ref[WorkerInternalState],
                        handle: Record => URIO[R, Any],
                        queue: Queue[Record]): URIO[R, Unit] =
      internalState.update(_.cleared) *>
        state.get.flatMap {
          case State.Running => queue.take.flatMap { record =>
            internalState.update(_.started) *>
              handle(record).uninterruptible *> loop(state, internalState, handle, queue)
          }

          case State.Paused(resume) =>
            resume.await *> loop(state, internalState, handle, queue)

          case State.ShuttingDown => ZIO.unit
        }
  }

}

case class WorkerInternalState(currentExecutionStarted: Option[Long]) {
  def cleared = copy(currentExecutionStarted = None)

  def started = copy(currentExecutionStarted = Some(System.currentTimeMillis()))
}

object WorkerInternalState {
  def empty = WorkerInternalState(None)
}

sealed trait SubmitResult

object SubmitResult {

  case object Submitted extends SubmitResult

  case object Rejected extends SubmitResult

}

sealed trait DispatcherMetric extends GreyhoundMetric

object DispatcherMetric {

  case class StartingWorker(partition: TopicPartition) extends DispatcherMetric

  case class StoppingWorker(partition: TopicPartition) extends DispatcherMetric

  case class SubmittingRecord[K, V](record: ConsumerRecord[K, V]) extends DispatcherMetric

  case class HandlingRecord[K, V](record: ConsumerRecord[K, V], timeInQueue: Long, group: Group) extends DispatcherMetric

  case class RecordHandled[K, V](record: ConsumerRecord[K, V], group: Group, duration: Duration) extends DispatcherMetric

}

case class DispatcherExposedState(workersState: Map[TopicPartition, WorkerExposedState]) {
  def totalQueuedTasksPerTopic: Map[Topic, Int] = workersState.groupBy(_._1.topic).map{case (topic, partitionStates) => (topic, partitionStates.map(_._2.queuedTasks).sum)}

  def maxTaskDuration = workersState.groupBy(_._1.topic).map{case (topic, partitionStates) => (topic, partitionStates.map(_._2.currentExecutionDuration.getOrElse(0L)).max)}

  def topics = workersState.groupBy(_._1.topic).keys
}

case class WorkerExposedState(queuedTasks: Int, currentExecutionDuration: Option[Long])

object DispatcherExposedState {
  def empty = DispatcherExposedState(Map.empty)
}