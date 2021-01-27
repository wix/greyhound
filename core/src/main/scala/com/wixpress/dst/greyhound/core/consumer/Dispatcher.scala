package com.wixpress.dst.greyhound.core.consumer

import java.util.concurrent.TimeUnit

import com.wixpress.dst.greyhound.core.consumer.Dispatcher.Record
import com.wixpress.dst.greyhound.core.consumer.DispatcherMetric._
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.consumer.SubmitResult._
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.{ClientId, Group, Topic}
import zio._
import zio.clock.Clock
import zio.duration.{Duration, _}
import zio.stm.{STM, TRef}

trait Dispatcher[-R] {
  def submit(record: Record): URIO[R with Env, SubmitResult]

  def resumeablePartitions(paused: Set[TopicPartition]): URIO[Clock, Set[TopicPartition]]

  def revoke(partitions: Set[TopicPartition]): URIO[GreyhoundMetrics with ZEnv, Unit]

  def pause: URIO[GreyhoundMetrics, Unit]

  def resume: URIO[GreyhoundMetrics, Unit]

  def shutdown: URIO[GreyhoundMetrics with ZEnv, Unit]

  def expose: URIO[Clock, DispatcherExposedState]

  def waitForCurrentRecordsCompletion: UIO[Unit]
}

object Dispatcher {
  type Record = ConsumerRecord[Chunk[Byte], Chunk[Byte]]

  def make[R](group: Group,
              clientId: ClientId,
              handle: Record => URIO[R, Any],
              lowWatermark: Int,
              highWatermark: Int,
              delayResumeOfPausedPartition: Long = 0): UIO[Dispatcher[R]] =
    for {
      state <- Ref.make[DispatcherState](DispatcherState.Running)
      workers <- Ref.make(Map.empty[TopicPartition, Worker])
    } yield new Dispatcher[R] {
      override def submit(record: Record): URIO[R with Env, SubmitResult] =
        for {
          _ <- report(SubmittingRecord(group, clientId, record))
          partition = TopicPartition(record)
          worker <- workerFor(partition)
          submitted <- worker.submit(record)
        } yield if (submitted) Submitted else Rejected

      override def resumeablePartitions(paused: Set[TopicPartition]): URIO[Clock, Set[TopicPartition]] =
        workers.get.flatMap { workers =>
          ZIO.foldLeft(paused)(Set.empty[TopicPartition]) { (acc, partition) =>
            workers.get(partition) match {
              case Some(worker) =>
                worker.expose
                  .map { state =>
                    if (state.queuedTasks <= lowWatermark && state.pausedPartitionTime > delayResumeOfPausedPartition) acc + partition
                    else acc
                  }.tap(tps => ZIO.when(tps.contains(partition))(worker.clearPausedPartitionDuration))

              case None => ZIO.succeed(acc + partition)
            }
          }
        }

      override def expose: URIO[Clock, DispatcherExposedState] =
        for {
          dispatcherState <- state.get
          workers <- workers.get
          workersState <- ZIO.foreach(workers) { case (tp, worker) => worker.expose.map(pending => (tp, pending)) }.map(_.toMap)
        } yield DispatcherExposedState(workersState, dispatcherState)


      override def waitForCurrentRecordsCompletion: UIO[Unit] =
        workers.get.flatMap(workers => ZIO.foreach(workers.values)(_.waitForCurrentExecutionCompletion)).unit

      override def revoke(partitions: Set[TopicPartition]): URIO[GreyhoundMetrics with ZEnv, Unit] =
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
          case DispatcherState.Running =>
            DispatcherState.Paused(resume)
        }
      } yield ()

      override def resume: URIO[GreyhoundMetrics, Unit] = state.modify {
        case DispatcherState.Paused(resume) => (resume.succeed(()).unit, DispatcherState.Running)
        case state => (ZIO.unit, state)
      }.flatten

      override def shutdown: URIO[GreyhoundMetrics with ZEnv, Unit] =
        state.modify(state => (state, DispatcherState.ShuttingDown)).flatMap {
          case DispatcherState.Paused(resume) => resume.succeed(()).unit
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
              worker.shutdown.timed.map(_._1).flatMap(duration =>
                report(WorkerStopped(group, clientId, partition, duration.toMillis)))
        }
    }

  sealed trait DispatcherState

  object DispatcherState {

    case object Running extends DispatcherState

    case class Paused(resume: Promise[Nothing, Unit]) extends DispatcherState

    case object ShuttingDown extends DispatcherState

  }

  case class Task(record: Record, complete: UIO[Unit])

  trait Worker {
    def submit(record: Record): URIO[Clock, Boolean]

    def expose: URIO[Clock, WorkerExposedState]

    def shutdown: UIO[Unit]

    def clearPausedPartitionDuration: UIO[Unit]

    def waitForCurrentExecutionCompletion: UIO[Unit]
  }

  object Worker {
    def make[R](status: Ref[DispatcherState],
                handle: Record => URIO[R, Any],
                capacity: Int,
                group: Group,
                clientId: ClientId,
                partition: TopicPartition): URIO[R with Env, Worker] = for {
      queue <- Queue.dropping[Record](capacity)
      internalState <- TRef.make(WorkerInternalState.empty).commit
      fiber <-
        pollOnce(status, internalState, handle, queue, group, clientId, partition)
          .interruptible.repeatWhile(_ == true).forkDaemon
    } yield new Worker {
      override def submit(record: Record): URIO[Clock, Boolean] =
        queue.offer(record)
          .tap(submitted => ZIO.when(!submitted) {
            clock.currentTime(TimeUnit.MILLISECONDS).flatMap(now =>
              internalState.update(s =>
                if (s.reachedHighWatermarkSince.nonEmpty) s else s.reachedHighWatermark(now)).commit)
          })

      override def expose: URIO[Clock, WorkerExposedState] =
        (queue.size zip internalState.get.commit)
          .flatMap { case (queued, state) =>
            clock.currentTime(TimeUnit.MILLISECONDS).map(currentTime =>
              WorkerExposedState(
                Math.max(0, queued),
                state.currentExecutionStarted.map(currentTime - _),
                state.reachedHighWatermarkSince.map(currentTime - _)))
          }

      override def shutdown: UIO[Unit] =
        fiber.interrupt.unit

      override def clearPausedPartitionDuration: UIO[Unit] = internalState.update(_.clearReachedHighWatermark).commit

      override def waitForCurrentExecutionCompletion: UIO[Unit] = internalState.get.flatMap(state => STM.check(state.currentExecutionStarted.isEmpty)).commit
    }

    private def pollOnce[R](state: Ref[DispatcherState],
                            internalState: TRef[WorkerInternalState],
                            handle: Record => URIO[R, Any],
                            queue: Queue[Record],
                            group: Group,
                            clientId: ClientId,
                            partition: TopicPartition): URIO[R with Env, Boolean] =
      internalState.update(s => s.cleared).commit *>
        state.get.flatMap {
          case DispatcherState.Running => queue.poll.flatMap {
            case Some(record) => report(TookRecordFromQueue(record, group, clientId)) *>
              internalState.update(_.started).commit *>
              handle(record).uninterruptible
                .as(true)
            case None => UIO(true).delay(5.millis)
          }
          case DispatcherState.Paused(resume) =>
            report(WorkerWaitingForResume(group, clientId, partition)) *>
              resume.await.timeout(30.seconds)
                .as(true)
          case DispatcherState.ShuttingDown =>
            UIO(false)
        }
  }

}

case class WorkerInternalState(currentExecutionStarted: Option[Long], reachedHighWatermarkSince: Option[Long]) {
  def cleared = copy(currentExecutionStarted = None)

  def started = copy(currentExecutionStarted = Some(System.currentTimeMillis()))

  def reachedHighWatermark(nowMs: Long): WorkerInternalState = copy(reachedHighWatermarkSince = Some(nowMs))

  def clearReachedHighWatermark = copy(reachedHighWatermarkSince = None)
}

object WorkerInternalState {
  def empty = WorkerInternalState(None, None)
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

  case class WorkerStopped(group: Group, clientId: ClientId, partition: TopicPartition, durationMs: Long) extends DispatcherMetric

  case class SubmittingRecord[K, V](group: Group, clientId: ClientId, record: ConsumerRecord[K, V]) extends DispatcherMetric

  case class HandlingRecord[K, V](group: Group, clientId: ClientId, record: ConsumerRecord[K, V], timeInQueue: Long) extends DispatcherMetric

  case class RecordHandled[K, V](group: Group, clientId: ClientId, record: ConsumerRecord[K, V], duration: Duration) extends DispatcherMetric

  case class TookRecordFromQueue(record: Record, group: Group, clientId: ClientId) extends DispatcherMetric

  case class WorkerWaitingForResume(group: Group, clientId: ClientId, partition: TopicPartition) extends DispatcherMetric

}

case class DispatcherExposedState(workersState: Map[TopicPartition, WorkerExposedState],
                                  state: Dispatcher.DispatcherState) {
  def totalQueuedTasksPerTopic: Map[Topic, Int] = workersState.groupBy(_._1.topic).map { case (topic, partitionStates) => (topic, partitionStates.map(_._2.queuedTasks).sum) }

  def maxTaskDuration = workersState.groupBy(_._1.topic).map { case (topic, partitionStates) => (topic, partitionStates.map(_._2.currentExecutionDuration.getOrElse(0L)).max) }

  def topics = workersState.groupBy(_._1.topic).keys
}

case class WorkerExposedState(queuedTasks: Int, currentExecutionDuration: Option[Long], pausedPartitionDuration: Option[Long] = None) {
  def pausedPartitionTime =
    pausedPartitionDuration.getOrElse(Long.MaxValue)
}

object DispatcherExposedState {
  def empty(state: Dispatcher.DispatcherState) = DispatcherExposedState(Map.empty, state)
}
