package com.wixpress.dst.greyhound.core.consumer

import java.util.concurrent.TimeUnit
import com.wixpress.dst.greyhound.core.consumer.Dispatcher.{Record, Records}
import com.wixpress.dst.greyhound.core.consumer.DispatcherMetric._
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.consumer.SubmitResult._
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordTopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown.ShutdownPromise
import com.wixpress.dst.greyhound.core.{ClientId, Group, Topic, TopicPartition}
import zio._
import zio.Clock
import zio.stm.{STM, TRef}

import java.lang.System.currentTimeMillis

trait Dispatcher[-R] {
  def submit(record: Record): URIO[R with Env, SubmitResult]

  def submitBatch(records: Records): URIO[R with Env, SubmitResult]

  def resumeablePartitions(paused: Set[TopicPartition]): URIO[Any, Set[TopicPartition]]

  def revoke(partitions: Set[TopicPartition]): URIO[GreyhoundMetrics, Unit]

  def pause: URIO[GreyhoundMetrics, Unit]

  def resume: URIO[GreyhoundMetrics, Unit]

  def shutdown: URIO[GreyhoundMetrics, Unit]

  def expose: URIO[Any, DispatcherExposedState]

  def waitForCurrentRecordsCompletion: UIO[Unit]
}

object Dispatcher {
  type Record  = ConsumerRecord[Chunk[Byte], Chunk[Byte]]
  type Records = Seq[Record]

  def make[R](
    group: Group,
    clientId: ClientId,
    handle: Record => URIO[R, Any],
    lowWatermark: Int,
    highWatermark: Int,
    drainTimeout: Duration = 15.seconds,
    delayResumeOfPausedPartition: Long = 0,
    consumerAttributes: Map[String, String] = Map.empty,
    workersShutdownRef: Ref[Map[TopicPartition, ShutdownPromise]],
    startPaused: Boolean = false,
    consumeInParallel: Boolean = false,
    maxParallelism: Int = 1,
    updateBatch: Chunk[Record] => UIO[Unit] = _ => ZIO.unit,
    currentGaps: Set[TopicPartition] => ZIO[GreyhoundMetrics, Nothing, Map[TopicPartition, Option[OffsetAndGaps]]] = _ =>
      ZIO.succeed(Map.empty)
  )(implicit trace: Trace): UIO[Dispatcher[R]] =
    for {
      p       <- Promise.make[Nothing, Unit]
      state   <- Ref.make[DispatcherState](if (startPaused) DispatcherState.Paused(p) else DispatcherState.Running)
      workers <- Ref.make(Map.empty[TopicPartition, Worker])
    } yield new Dispatcher[R] {
      override def submit(record: Record): URIO[R with Env, SubmitResult] =
        for {
          _         <- report(SubmittingRecord(group, clientId, record, consumerAttributes))
          partition  = RecordTopicPartition(record)
          worker    <- workerFor(partition, record.offset)
          submitted <- worker.submit(record)
        } yield if (submitted) Submitted else Rejected

      override def submitBatch(records: Records): URIO[R with Env, SubmitResult] =
        for {
          _               <- report(SubmittingRecordBatch(group, clientId, records.size, consumerAttributes))
          allSamePartition = records.map(r => RecordTopicPartition(r)).distinct.size == 1
          submitResult    <- if (allSamePartition) {
                               val partition = RecordTopicPartition(records.head)
                               for {
                                 worker    <- workerFor(partition, records.head.offset)
                                 submitted <- worker.submitBatch(records)
                               } yield submitted
                             } else ZIO.succeed(SubmitBatchResult(success = false, Some(records.minBy(_.offset))))

        } yield
          if (allSamePartition && submitResult.success) Submitted
          else RejectedBatch(submitResult.firstRejected.getOrElse(records.minBy(_.offset)))

      override def resumeablePartitions(paused: Set[TopicPartition]): URIO[Any, Set[TopicPartition]] =
        workers.get.flatMap { workers =>
          ZIO.foldLeft(paused)(Set.empty[TopicPartition]) { (acc, partition) =>
            workers.get(partition) match {
              case Some(worker) =>
                worker.expose
                  .map { state =>
                    if (state.queuedTasks <= lowWatermark && state.pausedPartitionTime > delayResumeOfPausedPartition) acc + partition
                    else acc
                  }
                  .tap(tps => ZIO.when(tps.contains(partition))(worker.clearPausedPartitionDuration))

              case None => ZIO.succeed(acc + partition)
            }
          }
        }

      override def expose: URIO[Any, DispatcherExposedState] =
        for {
          dispatcherState <- state.get
          workers         <- workers.get
          workersState    <- ZIO.foreach(workers) { case (tp, worker) => worker.expose.map(pending => (tp, pending)) }.map(_.toMap)
        } yield DispatcherExposedState(workersState, dispatcherState)

      override def waitForCurrentRecordsCompletion: UIO[Unit] =
        workers.get.flatMap(workers => ZIO.foreach(workers.values)(_.waitForCurrentExecutionCompletion)).unit

      override def revoke(partitions: Set[TopicPartition]): URIO[GreyhoundMetrics, Unit] =
        workers
          .modify { workers =>
            val revoked = workers.filterKeys(partitions.contains)
            val remaining = workers -- partitions

            (revoked, remaining)
          }
          .flatMap(shutdownWorkers)

      override def pause: URIO[GreyhoundMetrics, Unit] = for {
        resume <- Promise.make[Nothing, Unit]
        _      <- state.updateSome {
                    case DispatcherState.Running =>
                      DispatcherState.Paused(resume)
                  }
      } yield ()

      override def resume: URIO[GreyhoundMetrics, Unit] = state.modify {
        case DispatcherState.Paused(resume) => (resume.succeed(()).unit, DispatcherState.Running)
        case state                          => (ZIO.unit, state)
      }.flatten

      override def shutdown: URIO[GreyhoundMetrics, Unit] =
        state.modify(state => (state, DispatcherState.ShuttingDown)).flatMap {
          case DispatcherState.Paused(resume) => resume.succeed(()).unit
          case _                              => ZIO.unit
        } *> workers.get.flatMap(shutdownWorkers).ignore

      /**
       * This implementation is not fiber-safe. Since the worker is used per partition, and all operations performed on a single partition
       * are linearizable this is fine.
       */
      private def workerFor(partition: TopicPartition, offset: Long) =
        workers.get.flatMap { workers1 =>
          workers1.get(partition) match {
            case Some(worker) => ZIO.succeed(worker)
            case None         =>
              for {
                _               <- report(StartingWorker(group, clientId, partition, offset, consumerAttributes))
                worker          <- Worker.make(
                                     state,
                                     handleWithMetrics,
                                     highWatermark,
                                     group,
                                     clientId,
                                     partition,
                                     drainTimeout,
                                     consumerAttributes,
                                     consumeInParallel,
                                     maxParallelism,
                                     updateBatch,
                                     currentGaps
                                   )
                _               <- workers.update(_ + (partition -> worker))
                shutdownPromise <- AwaitShutdown.make
                _               <- workersShutdownRef.update(_.updated(partition, shutdownPromise))
              } yield worker
          }
        }

      private def handleWithMetrics(record: Record) =
        report(HandlingRecord(group, clientId, record, currentTimeMillis() - record.pollTime, consumerAttributes)) *>
          handle(record).timed.flatMap {
            case (duration, _) =>
              report(RecordHandled(group, clientId, record, duration, consumerAttributes))
          }

      private def shutdownWorkers(workers: Iterable[(TopicPartition, Worker)]) =
        ZIO
          .foreachParDiscard(workers) {
            case (partition, worker) =>
              report(StoppingWorker(group, clientId, partition, drainTimeout.toMillis, consumerAttributes)) *>
                workersShutdownRef.get.flatMap(_.get(partition).fold(ZIO.unit)(promise => promise.onShutdown.shuttingDown)) *>
                worker.shutdown
                  .catchSomeCause {
                    case _: Cause[InterruptedException] => ZIO.unit
                  } // happens on revoke - must not fail on it so we have visibility to worker completion
                  .timed
                  .map(_._1)
                  .flatMap(duration => report(WorkerStopped(group, clientId, partition, duration.toMillis, consumerAttributes)))
          }
          .resurrect
          .ignore
    }

  sealed trait DispatcherState

  object DispatcherState {

    case object Running extends DispatcherState

    case class Paused(resume: Promise[Nothing, Unit]) extends DispatcherState

    case object ShuttingDown extends DispatcherState

  }

  case class Task(record: Record, complete: UIO[Unit])

  trait Worker {
    def submit(record: Record): URIO[Any, Boolean]

    def submitBatch(records: Records): URIO[Any, SubmitBatchResult]

    def expose: URIO[Any, WorkerExposedState]

    def shutdown: URIO[Any, Unit]

    def clearPausedPartitionDuration: UIO[Unit]

    def waitForCurrentExecutionCompletion: UIO[Unit]
  }

  object Worker {
    def make[R](
      status: Ref[DispatcherState],
      handle: Record => URIO[R, Any],
      capacity: Int,
      group: Group,
      clientId: ClientId,
      partition: TopicPartition,
      drainTimeout: Duration,
      consumerAttributes: Map[String, String],
      consumeInParallel: Boolean,
      maxParallelism: Int,
      updateBatch: Chunk[Record] => UIO[Unit] = _ => ZIO.unit,
      currentGaps: Set[TopicPartition] => ZIO[GreyhoundMetrics, Nothing, Map[TopicPartition, Option[OffsetAndGaps]]]
    )(implicit trace: Trace): URIO[R with Env, Worker] = for {
      queue         <- Queue.dropping[Record](capacity)
      internalState <- TRef.make(WorkerInternalState.empty).commit
      fiber         <-
        (reportWorkerRunningInInterval(every = 60.seconds, internalState)(partition, group, clientId).forkDaemon *>
          (if (consumeInParallel)
              pollBatch(status, internalState, handle, queue, group, clientId, partition, consumerAttributes, maxParallelism, updateBatch, currentGaps)
            else pollOnce(status, internalState, handle, queue, group, clientId, partition, consumerAttributes))
              .repeatWhile(_ == true))
          .interruptible
          .forkDaemon
    } yield new Worker {
      override def submit(record: Record): URIO[Any, Boolean] =
        queue
          .offer(record)
          .tap(submitted =>
            ZIO.when(!submitted) {
              Clock
                .currentTime(TimeUnit.MILLISECONDS)
                .flatMap(now =>
                  internalState.update(s => if (s.reachedHighWatermarkSince.nonEmpty) s else s.reachedHighWatermark(now)).commit
                )
            }
          )

      override def submitBatch(
        records: Records
      ): URIO[Any, SubmitBatchResult] =
        queue
          .offerAll(records)
          .tap(notInserted =>
            ZIO.when(notInserted.nonEmpty) {
              Clock
                .currentTime(TimeUnit.MILLISECONDS)
                .flatMap(now =>
                  internalState.update(s => if (s.reachedHighWatermarkSince.nonEmpty) s else s.reachedHighWatermark(now)).commit
                )
            }
          )
          .map(rejected => {
            val isSuccess = rejected.isEmpty
            SubmitBatchResult(isSuccess, if (isSuccess) None else Some(rejected.minBy(_.offset)))
          })

      override def expose: URIO[Any, WorkerExposedState] = (queue.size zip internalState.get.commit)
        .flatMap {
          case (queued, state) =>
            Clock
              .currentTime(TimeUnit.MILLISECONDS)
              .map(currentTime =>
                WorkerExposedState(
                  Math.max(0, queued),
                  state.currentExecutionStarted.map(currentTime - _),
                  state.reachedHighWatermarkSince.map(currentTime - _)
                )
              )
        }

      override def shutdown: URIO[Any, Unit] =
        for {
          _       <- internalState.update(_.shutdown).commit
          timeout <- fiber.join.ignore.disconnect.timeout(drainTimeout)
          _       <- ZIO.when(timeout.isEmpty)(fiber.interruptFork)
        } yield ()

      override def clearPausedPartitionDuration: UIO[Unit] = internalState.update(_.clearReachedHighWatermark).commit

      override def waitForCurrentExecutionCompletion: UIO[Unit] =
        internalState.get.flatMap(state => STM.check(state.currentExecutionStarted.isEmpty)).commit
    }

    private def pollOnce[R](
      state: Ref[DispatcherState],
      internalState: TRef[WorkerInternalState],
      handle: Record => URIO[R, Any],
      queue: Queue[Record],
      group: Group,
      clientId: ClientId,
      partition: TopicPartition,
      consumerAttributes: Map[String, String]
    )(implicit trace: Trace): ZIO[R with GreyhoundMetrics, Any, Boolean] =
      internalState.update(s => s.cleared).commit *>
        state.get.flatMap {
          case DispatcherState.Running        =>
            queue.poll.flatMap {
              case Some(record) =>
                report(TookRecordFromQueue(record, group, clientId, consumerAttributes)) *>
                  ZIO
                    .attempt(currentTimeMillis())
                    .flatMap(t => internalState.updateAndGet(_.startedWith(t)).commit)
                    .tapBoth(
                      e => report(FailToUpdateCurrentExecutionStarted(record, group, clientId, consumerAttributes, e)),
                      t => report(CurrentExecutionStartedEvent(partition, group, clientId, t.currentExecutionStarted))
                    ) *> handle(record).interruptible.ignore *> isActive(internalState)
              case None         => isActive(internalState).delay(5.millis)
            }
          case DispatcherState.Paused(resume) =>
            report(WorkerWaitingForResume(group, clientId, partition, consumerAttributes)) *> resume.await.timeout(30.seconds) *>
              isActive(internalState)
          case DispatcherState.ShuttingDown   =>
            ZIO.succeed(false)
        }

    private def pollBatch[R](
      state: Ref[DispatcherState],
      internalState: TRef[WorkerInternalState],
      handle: Record => URIO[R, Any],
      queue: Queue[Record],
      group: Group,
      clientId: ClientId,
      partition: TopicPartition,
      consumerAttributes: Map[String, String],
      maxParallelism: Int,
      updateBatch: Chunk[Record] => UIO[Unit],
      currentGaps: Set[TopicPartition] => ZIO[GreyhoundMetrics, Nothing, Map[TopicPartition, Option[OffsetAndGaps]]]
    )(implicit trace: Trace): ZIO[R with GreyhoundMetrics, Any, Boolean] =
      internalState.update(s => s.cleared).commit *>
        state.get.flatMap {
          case DispatcherState.Running        =>
            queue.takeAll.flatMap {
              case records if records.nonEmpty =>
                handleBatch(
                  records,
                  internalState,
                  handle,
                  group,
                  clientId,
                  partition,
                  consumerAttributes,
                  maxParallelism,
                  updateBatch,
                  currentGaps
                )
              case _                           => isActive(internalState).delay(5.millis)
            }
          case DispatcherState.Paused(resume) =>
            report(WorkerWaitingForResume(group, clientId, partition, consumerAttributes)) *> resume.await.timeout(30.seconds) *>
              isActive(internalState)
          case DispatcherState.ShuttingDown   =>
            ZIO.succeed(false)
        }
    private def handleBatch[R](
      records: Chunk[Record],
      internalState: TRef[WorkerInternalState],
      handle: Record => URIO[R, Any],
      group: Group,
      clientId: ClientId,
      partition: TopicPartition,
      consumerAttributes: Map[ClientId, ClientId],
      maxParallelism: RuntimeFlags,
      updateBatch: Chunk[Record] => UIO[Unit],
      currentGaps: Set[TopicPartition] => ZIO[GreyhoundMetrics, Nothing, Map[TopicPartition, Option[OffsetAndGaps]]]
    ): ZIO[R with GreyhoundMetrics, Throwable, Boolean] =
      for {
        _                <- report(TookAllRecordsFromQueue(records.size, records, group, clientId, consumerAttributes))
        _                <- ZIO
                              .attempt(currentTimeMillis())
                              .flatMap(t => internalState.updateAndGet(_.startedWith(t)).commit)
                              .tapBoth(
                                e => report(FailToUpdateParallelCurrentExecutionStarted(records.size, group, clientId, consumerAttributes, e)),
                                t => report(CurrentExecutionStartedEvent(partition, group, clientId, t.currentExecutionStarted))
                              )
        groupedRecords    = records.groupBy(_.key).values // todo: add sub-grouping for records without key
        latestCommitGaps <- currentGaps(records.map(r => TopicPartition(r.topic, r.partition)).toSet)
        _                <- ZIO
                              .foreachParDiscard(groupedRecords)(sameKeyRecords =>
                                ZIO.foreach(sameKeyRecords) { record =>
                                  if (shouldRecordBeHandled(record, latestCommitGaps)) {
                                    handle(record).interruptible.ignore *> updateBatch(sameKeyRecords).interruptible
                                  } else
                                    report(SkippedPreviouslyHandledRecord(record, group, clientId, consumerAttributes))

                                }
                              )
                              .withParallelism(maxParallelism)
        res              <- isActive(internalState)
      } yield res
  }

  private def shouldRecordBeHandled(record: Record, maybeGaps: Map[TopicPartition, Option[OffsetAndGaps]]): Boolean = {
    maybeGaps.get(TopicPartition(record.topic, record.partition)) match {
      case Some(maybeOffsetAndGapsForPartition) =>
        maybeOffsetAndGapsForPartition match {
          case Some(offsetAndGapsForPartition) if offsetAndGapsForPartition.gaps.nonEmpty =>
            record.offset > offsetAndGapsForPartition.offset || offsetAndGapsForPartition.gaps.exists(_.contains(record.offset))
          case _                                                                          => true
        }
      case None                                 => true
    }
  }

  private def reportWorkerRunningInInterval(
    every: zio.Duration,
    state: TRef[WorkerInternalState]
  )(topicPartition: TopicPartition, group: Group, clientId: String): URIO[GreyhoundMetrics, Unit] = {
    for {
      start <- Clock
                 .currentTime(TimeUnit.MILLISECONDS)
      id    <- ZIO.succeed(scala.util.Random.alphanumeric.take(10).mkString)
      _     <- GreyhoundMetrics
                 .report(WorkerRunning(topicPartition, group, clientId, start, id))
                 .repeat(Schedule.recurUntilZIO((_: Any) => state.get.map(_.shuttingDown).commit) && Schedule.spaced(every))
                 .unit
    } yield ()
  }

  private def isActive[R](internalState: TRef[WorkerInternalState])(implicit trace: Trace) =
    internalState.get.map(_.shuttingDown).commit.negate
}

case class WorkerInternalState(
  currentExecutionStarted: Option[Long],
  reachedHighWatermarkSince: Option[Long],
  shuttingDown: Boolean = false
) {
  def cleared = copy(currentExecutionStarted = None)

  def started                 = copy(currentExecutionStarted = Some(currentTimeMillis()))
  def startedWith(time: Long) = copy(currentExecutionStarted = Some(time))

  def reachedHighWatermark(nowMs: Long): WorkerInternalState = copy(reachedHighWatermarkSince = Some(nowMs))

  def clearReachedHighWatermark = copy(reachedHighWatermarkSince = None)

  def shutdown = copy(shuttingDown = true)
}

object WorkerInternalState {
  def empty = WorkerInternalState(None, None)
}

sealed trait SubmitResult

object SubmitResult {

  case object Submitted extends SubmitResult

  case object Rejected extends SubmitResult

  case class RejectedBatch(firstRejected: Record) extends SubmitResult

}

case class SubmitBatchResult(success: Boolean, firstRejected: Option[Record]) extends SubmitResult

sealed trait DispatcherMetric extends GreyhoundMetric

object DispatcherMetric {

  case class WorkerRunning(topicPartition: TopicPartition, group: Group, clientId: String, started: Long, id: String)
      extends DispatcherMetric

  case class StartingWorker(group: Group, clientId: ClientId, partition: TopicPartition, firstOffset: Long, attributes: Map[String, String])
      extends DispatcherMetric

  case class StoppingWorker(
    group: Group,
    clientId: ClientId,
    partition: TopicPartition,
    drainTimeoutMs: Long,
    attributes: Map[String, String]
  ) extends DispatcherMetric

  case class WorkerStopped(group: Group, clientId: ClientId, partition: TopicPartition, durationMs: Long, attributes: Map[String, String])
      extends DispatcherMetric

  case class SubmittingRecord[K, V](group: Group, clientId: ClientId, record: ConsumerRecord[K, V], attributes: Map[String, String])
      extends DispatcherMetric

  case class SubmittingRecordBatch[K, V](group: Group, clientId: ClientId, numRecords: Int, attributes: Map[String, String])
      extends DispatcherMetric

  case class HandlingRecord[K, V](
    group: Group,
    clientId: ClientId,
    record: ConsumerRecord[K, V],
    timeInQueue: Long,
    attributes: Map[String, String]
  ) extends DispatcherMetric

  case class RecordHandled[K, V](
    group: Group,
    clientId: ClientId,
    record: ConsumerRecord[K, V],
    duration: Duration,
    attributes: Map[String, String]
  ) extends DispatcherMetric

  case class TookRecordFromQueue(record: Record, group: Group, clientId: ClientId, attributes: Map[String, String]) extends DispatcherMetric
  case class TookAllRecordsFromQueue(
    numRecords: Int,
    records: Chunk[Record],
    group: Group,
    clientId: ClientId,
    attributes: Map[String, String]
  ) extends DispatcherMetric
  case class FailToUpdateCurrentExecutionStarted(
    record: Record,
    group: Group,
    clientId: ClientId,
    attributes: Map[String, String],
    e: Throwable
  ) extends DispatcherMetric
  case class FailToUpdateParallelCurrentExecutionStarted(
    numRecords: Int,
    group: Group,
    clientId: ClientId,
    attributes: Map[String, String],
    e: Throwable
  ) extends DispatcherMetric

  case class WorkerWaitingForResume(group: Group, clientId: ClientId, partition: TopicPartition, attributes: Map[String, String])
      extends DispatcherMetric

  case class CurrentExecutionStartedEvent(
    partition: TopicPartition,
    group: Group,
    clientId: ClientId,
    currentExecutionStarted: Option[Long]
  ) extends DispatcherMetric

  case class SkippedPreviouslyHandledRecord(record: Record, group: Group, clientId: ClientId, attributes: Map[String, String])
      extends DispatcherMetric

}

case class DispatcherExposedState(workersState: Map[TopicPartition, WorkerExposedState], state: Dispatcher.DispatcherState) {
  def totalQueuedTasksPerTopic: Map[Topic, Int] =
    workersState.groupBy(_._1.topic).map { case (topic, partitionStates) => (topic, partitionStates.map(_._2.queuedTasks).sum) }

  def maxTaskDuration = workersState.groupBy(_._1.topic).map {
    case (topic, partitionStates) => (topic, partitionStates.map(_._2.currentExecutionDuration.getOrElse(0L)).max)
  }

  def topics = workersState.groupBy(_._1.topic).keys
}

case class WorkerExposedState(queuedTasks: Int, currentExecutionDuration: Option[Long], pausedPartitionDuration: Option[Long] = None) {
  def pausedPartitionTime =
    pausedPartitionDuration.getOrElse(Long.MaxValue)
}

object DispatcherExposedState {
  def empty(state: Dispatcher.DispatcherState) = DispatcherExposedState(Map.empty, state)
}
