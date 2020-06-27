package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Dispatcher.{Record, State}
import com.wixpress.dst.greyhound.core.consumer.DispatcherMetric._
import com.wixpress.dst.greyhound.core.consumer.SubmitResult._
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.{ClientId, Group, Topic}
import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.duration._

trait Dispatcher[-R] {
  def submit(record: Record): URIO[R with Clock with GreyhoundMetrics,SubmitResult]

  def resumeablePartitions(paused: Set[TopicPartition]): UIO[Set[TopicPartition]]

  def revoke(partitions: Set[TopicPartition]): URIO[GreyhoundMetrics,Unit]

  def pause: URIO[GreyhoundMetrics, Unit]

  def resume: URIO[GreyhoundMetrics, Unit]

  def shutdown: URIO[GreyhoundMetrics, Unit]

  def expose: UIO[DispatcherExposedState]

}

object Dispatcher {
  type Record = ConsumerRecord[Chunk[Byte], Chunk[Byte]]

  def make[R](group: Group,
              clientId: ClientId,
              handle: Record => URIO[R, Any],
              lowWatermark: Int,
              highWatermark: Int): UIO[Dispatcher[R]] =
    for {
      state <- Ref.make[State](State.Running)
      workers <- Ref.make(Map.empty[TopicPartition, Worker])
    } yield new Dispatcher[R] {
      override def submit(record: Record): URIO[R with Clock with GreyhoundMetrics,SubmitResult] =
        for {
          _ <- report(SubmittingRecord(group, clientId, record))
          partition = TopicPartition(record)
          worker <- workerFor(partition)
          submitted <- worker.submit(record)
        } yield if (submitted) Submitted else Rejected

      override def resumeablePartitions(paused: Set[TopicPartition]): UIO[Set[TopicPartition]] =
        workers.get.flatMap { workers =>
          ZIO.foldLeft(paused)(Set.empty[TopicPartition]) { (acc, partition) =>
            workers.get(partition) match {
              case Some(worker) =>
                worker.expose
                  .map { state =>
                    if (state.queuedTasks <= lowWatermark) acc + partition
                    else acc
                  }

              case None => ZIO.succeed(acc + partition)
            }
          }
        }

      override def expose: UIO[DispatcherExposedState] =
        for {
          dispatcherState <- state.get
          workers <- workers.get
          workersState <- ZIO.foreach(workers) { case (tp, worker) => worker.expose.map(pending => (tp, pending)) }.map(_.toMap)
        } yield DispatcherExposedState(workersState, dispatcherState)

      override def revoke(partitions: Set[TopicPartition]): URIO[GreyhoundMetrics, Unit] =
        workers.modify { workers =>
          partitions.foldLeft((List.empty[(TopicPartition, Worker)], workers)) {
            case ((revoked, remaining), partition) =>
              remaining.get(partition) match {
                case Some(worker) => ((partition, worker) :: revoked, remaining - partition)
                case None => (revoked, remaining)
              }
          }
        }.flatMap(shutdownWorkers)

      override def pause: URIO[GreyhoundMetrics, Unit] = for {
        resume <- Promise.make[Nothing, Unit]
        _ <- state.updateSome {
          case State.Running =>
            State.Paused(resume)
        }
      } yield ()

      override def resume: URIO[GreyhoundMetrics, Unit] = state.modify {
        case State.Paused(resume) => (resume.succeed(()).unit, State.Running)
        case state => (ZIO.unit, state)
      }.flatten

      override def shutdown: URIO[GreyhoundMetrics, Unit] =
        state.modify(state => (state, State.ShuttingDown)).flatMap {
          case State.Paused(resume) => resume.succeed(()).unit
          case _ => ZIO.unit
        } *> workers.get.flatMap(shutdownWorkers)

      /**
       * This implementation is not fiber-safe. Since the worker is used per partition,
       * and all operations performed on a single partition are linearizable this is fine.
       */
      private def workerFor(partition: TopicPartition) =
        workers.get.flatMap { workers1 =>
          workers1.get(partition) match {
            case Some(worker) => ZIO.succeed(worker)
            case None => for {
              _ <- report(StartingWorker(group, clientId, partition))
              worker <- Worker.make(state, handleWithMetrics, highWatermark, group, clientId, partition)
              _ <- workers.update(_ + (partition -> worker))
            } yield worker
          }
        }

      private def handleWithMetrics(record: Record) =
        report(HandlingRecord(group, clientId, record, System.currentTimeMillis() - record.pollTime)) *>
          handle(record).timed.flatMap {
            case (duration, _) =>
              report(RecordHandled(group, clientId, record, duration))
          }

      private def shutdownWorkers(workers: Iterable[(TopicPartition, Worker)]) =
        ZIO.foreachPar_(workers) {
          case (partition, worker) =>
            report(StoppingWorker(group, clientId, partition)) *>
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
                capacity: Int,
                group: Group,
                clientId: ClientId,
                partition: TopicPartition): URIO[R with Clock with GreyhoundMetrics, Worker] = for {
      queue <- Queue.dropping[Record](capacity)
      internalState <- Ref.make(WorkerInternalState.empty)
      fiber <-
        pollOnce(status, internalState, handle, queue, group, clientId, partition)
          .interruptible.doWhile(_ == true).forkDaemon
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

    private def pollOnce[R](state: Ref[State],
                            internalState: Ref[WorkerInternalState],
                            handle: Record => URIO[R, Any],
                            queue: Queue[Record],
                            group: Group,
                            clientId: ClientId,
                            partition: TopicPartition): URIO[R with Clock with GreyhoundMetrics, Boolean] =
      internalState.update(_.cleared) *>
        state.get.flatMap {
          case State.Running => queue.take.flatMap { record =>
            report(TookRecordFromQueue(record, group, clientId)) *>
              internalState.update(_.started) *>
              handle(record).uninterruptible
                .as(true)
          }
          case State.Paused(resume) =>
            report(WorkerWaitingForResume(group, clientId, partition)) *>
              resume.await.timeout(30.seconds)
                .as(true)
          case State.ShuttingDown =>
            UIO(false)
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

  case class StartingWorker(group: Group, clientId: ClientId, partition: TopicPartition) extends DispatcherMetric

  case class StoppingWorker(group: Group, clientId: ClientId, partition: TopicPartition) extends DispatcherMetric

  case class SubmittingRecord[K, V](group: Group, clientId: ClientId, record: ConsumerRecord[K, V]) extends DispatcherMetric

  case class HandlingRecord[K, V](group: Group, clientId: ClientId, record: ConsumerRecord[K, V], timeInQueue: Long) extends DispatcherMetric

  case class RecordHandled[K, V](group: Group, clientId: ClientId, record: ConsumerRecord[K, V], duration: Duration) extends DispatcherMetric

  case class TookRecordFromQueue(record: Record, group: Group, clientId: ClientId) extends DispatcherMetric

  case class WorkerWaitingForResume(group: Group, clientId: ClientId, partition: TopicPartition) extends DispatcherMetric

}

case class DispatcherExposedState(workersState: Map[TopicPartition, WorkerExposedState], state: Dispatcher.State) {
  def totalQueuedTasksPerTopic: Map[Topic, Int] = workersState.groupBy(_._1.topic).map { case (topic, partitionStates) => (topic, partitionStates.map(_._2.queuedTasks).sum) }

  def maxTaskDuration = workersState.groupBy(_._1.topic).map { case (topic, partitionStates) => (topic, partitionStates.map(_._2.currentExecutionDuration.getOrElse(0L)).max) }

  def topics = workersState.groupBy(_._1.topic).keys
}

case class WorkerExposedState(queuedTasks: Int, currentExecutionDuration: Option[Long])

object DispatcherExposedState {
  def empty(state: Dispatcher.State) = DispatcherExposedState(Map.empty, state)
}
