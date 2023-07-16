package com.wixpress.dst.greyhound.core.consumer.batched

import java.time.Instant
import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.consumer.EventLoopMetric.{PausingEventLoop, ResumingEventLoop, StartingEventLoop, StoppingEventLoop}
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.batched.BatchEventLoop.{ExtraEnv, Handler, Record}
import com.wixpress.dst.greyhound.core.consumer.batched.BatchEventLoopMetric._
import com.wixpress.dst.greyhound.core.consumer.batched.BatchEventLoopState.{PauseResume, PendingRecords}
import com.wixpress.dst.greyhound.core.consumer.domain.{BatchRecordHandler, ConsumerRecord, ConsumerRecordBatch, ConsumerSubscription, HandleError}
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.{ClientId, Group, Offset, Partition, TopicPartition}
import zio.Clock
import zio.Clock.currentDateTime
import zio.stm.{STM, TRef}
import zio.{Chunk, Exit, Fiber, Promise, Ref, UIO, URIO, ZIO}

import scala.reflect.ClassTag
import zio._

trait BatchEventLoop[R] extends Resource[R] {
  def state(implicit trace: Trace): UIO[EventLoopExposedState]

  def rebalanceListener: RebalanceListener[Any]

  def requestSeek(toOffsets: Map[TopicPartition, Offset])(implicit trace: Trace): UIO[Unit]

  def shutdown()(implicit trace: Trace): UIO[Unit]
}

private[greyhound] class BatchEventLoopImpl[R](
  group: Group,
  clientId: ClientId,
  consumer: Consumer,
  handler: Handler[R, Any],
  retry: Option[BatchRetryConfig] = None,
  config: BatchEventLoopConfig,
  elState: BatchEventLoopState,
  val rebalanceListener: RebalanceListener[Any],
  loopFiberRef: Ref[Option[Fiber[_, _]]],
  capturedR: ExtraEnv,
  seekRequests: Ref[Map[TopicPartition, Offset]]
) extends BatchEventLoop[R] {

  private val consumerAttributes = consumer.config(zio.Trace.empty).consumerAttributes

  override def state(implicit trace: Trace): UIO[EventLoopExposedState] = elState.eventLoopState()

  private def report(metric: GreyhoundMetric)(implicit trace: Trace) = GreyhoundMetrics.report(metric).provide(ZLayer.succeed(capturedR))

  override def pause(implicit trace: Trace): URIO[R, Unit] =
    report(PausingEventLoop(clientId, group, consumerAttributes)) *> elState.pause()

  override def resume(implicit trace: Trace): URIO[R, Unit] =
    report(ResumingEventLoop(clientId, group, consumerAttributes)) *> elState.resume()

  override def isAlive(implicit trace: Trace): UIO[Boolean] =
    loopFiberRef.get.flatMap(_.fold(ZIO.succeed(false))(_.poll.map {
      case Some(Exit.Failure(_)) => false
      case _                     => true
    }))

  def startPolling()(implicit trace: Trace): URIO[R with GreyhoundMetrics, Unit] = for {
    _     <- report(StartingEventLoop(clientId, group, consumerAttributes))
    fiber <- pollOnce.sandbox.ignore
               .repeatWhileZIO(_ => elState.notShutdown)
               .forkDaemon
    _     <- loopFiberRef.set(Some(fiber))
  } yield ()

  def shutdown()(implicit trace: Trace): UIO[Unit] = for {
    _ <- report(StoppingEventLoop(clientId, group, consumerAttributes))
    _ <- elState.shutdown()
  } yield ()

  private def pollOnce(implicit trace: Trace): URIO[R with GreyhoundMetrics, Unit] =
    elState
      .awaitRunning()
      .timed
      .provideEnvironment(ZEnvironment(capturedR))
      .flatMap {
        case (waited, _) =>
          for {
            _ <- ZIO.when(waited > 1.second)(report(EventloopWaitedForResume(group, clientId, waited, consumerAttributes)))
            _ <- pollAndHandle()
            _ <- seekIfRequested()
            _ <- commitOffsets()
          } yield ()
      }

  private def seekIfRequested()(implicit trace: Trace) =
    seekRequests.get
      .flatMap(offsetSeeks =>
        ZIO.when(offsetSeeks.nonEmpty)(
          consumer.seek(offsetSeeks) *>
            elState.offsets
              .update(offsetSeeks)
              .tap(_ => seekRequests.set(Map.empty))
        )
      )

  private def pollAndHandle()(implicit trace: Trace): URIO[R with GreyhoundMetrics, Unit] = for {
    _          <- pauseAndResume().ignore
    allRecords <- consumer
                    .poll(config.fetchTimeout)
                    .catchAll(_ => ZIO.succeed(Nil))
    seeks      <- seekRequests.get.map(_.keySet)
    records     = allRecords.filterNot(record => seeks.contains(record.topicPartition))
    _          <- handleRecords(records).timed
                    .tap { case (duration, _) => report(FullBatchHandled(clientId, group, records.toSeq, duration, consumerAttributes)) }
  } yield ()

  private def pauseAndResume()(implicit trace: Trace) = for {
    pr <- elState.shouldPauseAndResume()
    _  <- ZIO.when(pr.toPause.nonEmpty)(consumer.pause(pr.toPause) *> elState.partitionsPaused(pr.toPause))
    _  <- ZIO.when(pr.toResume.nonEmpty)(consumer.resume(pr.toResume) *> elState.partitionsResumed(pr.toResume))
  } yield ()

  private def handleRecords(polled: Records)(implicit trace: Trace): ZIO[R, Nothing, Unit] = {
    retry match {
      case None        => handleWithNoRetry(polled)
      case Some(retry) => handleWithRetry(polled, retry.backoff)
    }
  }

  private def handleWithRetry(polled: Records, retryBackoff: Duration)(implicit trace: Trace): ZIO[R, Nothing, Unit] = {
    for {
      forRetryPairs                     <- elState.readyForRetryAndBlocked(retryBackoff).provideEnvironment(ZEnvironment(capturedR))
      (forRetry, stillPending)           = forRetryPairs
      blockedPartitions                  = stillPending.keySet
      (polledReadyToGo, polledOnBlocked) = polled.partition(r => !blockedPartitions(r.topicPartition))
      polledByPartition                  = recordsByPartition(polledReadyToGo)
      // we shouldn't get polled records for partitions with pending records, as they should be paused
      // but if we do for some reason - we append them to the pending records
      _                                 <- elState.appendPending(polledOnBlocked)
      // some for partitions ready for retry
      readyToHandle                      = concatByKey(forRetry, polledByPartition)
      _                                 <- report(EventLoopIteration(group, clientId, polled.toSeq, forRetry, stillPending, consumerAttributes))
      _                                 <- handleInParallelWithRetry(readyToHandle)
    } yield ()
  }

  private def handleInParallelWithRetry(readyToHandle: Map[TopicPartition, Seq[Record]])(implicit trace: Trace): ZIO[R, Nothing, Unit] = {
    ZIO
      .foreachParDiscard(readyToHandle) {
        case (tp, records) =>
          (handler.handle(ConsumerRecordBatch(tp.topic, tp.partition, records)) *> elState.markHandled(tp, records)).catchAllCause {
            cause =>
              val forceNoRetry = cause.failureOption.fold(false)(_.forceNoRetry)
              report(HandleAttemptFailed(group, clientId, tp.topic, tp.partition, records, !forceNoRetry, consumerAttributes)) *> {
                if (forceNoRetry) elState.markHandled(tp, records)
                else elState.attemptFailed(tp, records).provideEnvironment(ZEnvironment(capturedR))
              }
          }
      }
      .withParallelism(config.parallelism)
  }

  private def concatByKey[K, V](a: Map[K, Seq[V]], b: Map[K, Seq[V]]) = {
    b.foldLeft(a) {
      case (acc, (tp, recs)) =>
        acc + (tp -> (acc.getOrElse(tp, Nil) ++ recs))
    }
  }

  private def handleWithNoRetry(polled: Records)(implicit trace: Trace) = {
    ZIO
      .foreachParDiscard(recordsByPartition(polled)) {
        case (tp, records) =>
          handler.handle(ConsumerRecordBatch(tp.topic, tp.partition, records)).sandbox.ignore *> elState.offsets.update(records)
      }
      .withParallelism(config.parallelism)
  }

  private def commitOffsets()(implicit trace: Trace): UIO[Unit] =
    elState.offsets.committable.flatMap { committable =>
      consumer.commit(committable).provide(ZLayer.succeed(capturedR)).catchAllCause { _ => elState.offsets.update(committable) }
    }

  private def recordsByPartition(records: Records): Map[TopicPartition, Seq[Record]] = {
    records.toSeq.groupBy(r => TopicPartition(r.topic, r.partition))
  }

  override def requestSeek(toOffsets: Map[TopicPartition, Offset])(implicit trace: Trace): UIO[Unit] =
    seekRequests.update(_ ++ toOffsets)
}

object BatchEventLoop {
  type Handler[-R, +E] = BatchRecordHandler[R, E, Chunk[Byte], Chunk[Byte]]
  type Record          = ConsumerRecord[Chunk[Byte], Chunk[Byte]]

  type ExtraEnv = GreyhoundMetrics

  def make[R](
    group: Group,
    initialSubscription: ConsumerSubscription,
    consumer: Consumer,
    handler: Handler[R, Any],
    clientId: ClientId,
    retry: Option[BatchRetryConfig] = None,
    config: BatchEventLoopConfig = BatchEventLoopConfig.Default
  )(implicit trace: Trace): RIO[R with ExtraEnv with Scope, BatchEventLoop[R]] = {
    val start = for {
      state               <- BatchEventLoopState.make
      partitionsAssigned  <- Promise.make[Nothing, Unit]
      rebalanceListener    = listener(state, config, partitionsAssigned)
      _                   <- subscribe[R](initialSubscription, rebalanceListener)(consumer)
      fiberRef            <- Ref.make(Option.empty[Fiber[_, _]])
      instrumentedHandler <- handlerWithMetrics(group, clientId, handler, consumer.config.consumerAttributes)
      env                 <- ZIO.environment[ExtraEnv]
      _                   <- ZIO.when(config.startPaused)(state.pause())
      seekRequests        <- Ref.make(Map.empty[TopicPartition, Offset])
      eventLoop            = new BatchEventLoopImpl[R](
                               group,
                               clientId,
                               consumer,
                               instrumentedHandler,
                               retry,
                               config,
                               state,
                               rebalanceListener,
                               fiberRef,
                               env.get,
                               seekRequests
                             )
      _                   <- eventLoop.startPolling()
      _                   <- ZIO.when(!config.startPaused)(partitionsAssigned.await)
    } yield eventLoop

    ZIO.acquireRelease(start)(_.shutdown())
  }

  private def listener(
    state: BatchEventLoopState,
    config: BatchEventLoopConfig,
    partitionsAssigned: Promise[Nothing, Unit]
  ): RebalanceListener[Any] = {
    config.rebalanceListener *>
      new RebalanceListener[Any] {
        override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
          implicit trace: Trace
        ): URIO[Any, DelayedRebalanceEffect] = {
          state.partitionsRevoked(partitions).as(DelayedRebalanceEffect.unit)
        }

        override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(implicit trace: Trace): UIO[Any] =
          partitionsAssigned.succeed(())
      }
  }

  private def handlerWithMetrics[R, E](
    group: Group,
    clientId: ClientId,
    handler: Handler[R, E],
    consumerAttributes: Map[String, String]
  )(implicit trace: Trace): ZIO[ExtraEnv, Nothing, BatchRecordHandler[R, E, Chunk[Byte], Chunk[Byte]]] =
    ZIO.environment[ExtraEnv].map { env =>
      def report(m: GreyhoundMetric) = GreyhoundMetrics.report(m).provideEnvironment(env)

      def currentTime = currentDateTime.map(_.toInstant.toEpochMilli).provideEnvironment(env)

      val nanoTime = Clock.nanoTime
      new Handler[R, E] {
        override def handle(records: ConsumerRecordBatch[Chunk[Byte], Chunk[Byte]]): ZIO[R, HandleError[E], Any] = {
          val effect = for {
            now         <- currentTime
            sinceProduce = now - records.records.headOption.fold(now)(_.producedTimestamp)
            sincePoll    = now - records.records.headOption.fold(now)(_.pollTime)
            _           <- report(
                             HandlingRecords(
                               records.topic,
                               group,
                               records.partition,
                               clientId,
                               records.records,
                               sinceProduce,
                               sincePoll,
                               consumerAttributes
                             )
                           ) *>
                             handler.handle(records).timedWith(nanoTime).flatMap {
                               case (duration, _) =>
                                 report(
                                   RecordsHandled(records.topic, group, records.partition, clientId, records.records, duration, consumerAttributes)
                                 )
                             }
          } yield ()
          effect
        }
      }
    }
}

case class EventLoopExposedState(
  running: Boolean,
  shutdown: Boolean,
  pendingRecords: Map[TopicPartition, Int],
  pausedPartitions: Set[TopicPartition]
) {
  def paused = !running
}

case class BatchEventLoopConfig(
  fetchTimeout: Duration,
  rebalanceListener: RebalanceListener[Any],
  parallelism: Int = 8,
  startPaused: Boolean
) {
  def withParallelism(n: Int) = copy(parallelism = n)
}

object BatchEventLoopConfig {
  val Default = BatchEventLoopConfig(fetchTimeout = 500.millis, rebalanceListener = RebalanceListener.Empty, startPaused = false)
}

sealed trait BatchEventLoopMetric extends GreyhoundMetric

object BatchEventLoopMetric {
  case class FullBatchHandled[K, V](
    clientId: ClientId,
    group: Group,
    records: Seq[ConsumerRecord[K, V]],
    duration: Duration,
    attributes: Map[String, String]
  ) extends BatchEventLoopMetric

  case class EventLoopFailedToSeek(seekRequests: Map[TopicPartition, Offset], t: IllegalStateException) extends BatchEventLoopMetric

  case class RecordsHandled[K, V](
    topic: String,
    group: Group,
    partition: Partition,
    clientId: ClientId,
    records: Seq[ConsumerRecord[K, V]],
    duration: Duration,
    attributes: Map[String, String]
  ) extends BatchEventLoopMetric

  case class HandlingRecords[K, V](
    topic: String,
    group: Group,
    partition: Partition,
    clientId: ClientId,
    records: Seq[ConsumerRecord[K, V]],
    timeSinceProduceMs: Long,
    timeSincePollMs: Long,
    attributes: Map[String, String]
  ) extends BatchEventLoopMetric

  case class RecordsPendingForRetry[K, V](
    topic: String,
    group: Group,
    partition: Int,
    clientId: ClientId,
    records: Seq[ConsumerRecord[K, V]],
    attributes: Map[String, String]
  ) extends BatchEventLoopMetric

  case class EventloopWaitedForResume(group: Group, clientId: ClientId, waitedFor: Duration, attributes: Map[String, String])
      extends BatchEventLoopMetric

  case class Pending(count: Int, lastAttempt: Option[Instant], attempts: Int)

  case class EventLoopIteration(
    group: Group,
    clientId: ClientId,
    polled: Map[String, Int],
    readyForRetry: Map[String, Int],
    stillPending: Map[String, Pending],
    attributes: Map[String, String]
  ) extends BatchEventLoopMetric

  def EventLoopIteration(
    group: Group,
    clientId: ClientId,
    polled: Seq[Record],
    readyForRetry: Map[TopicPartition, Seq[Record]],
    stillPending: Map[TopicPartition, PendingRecords],
    attributes: Map[String, String]
  ): EventLoopIteration =
    EventLoopIteration(
      group,
      clientId,
      countsByPartition(polled),
      countsByPartition(readyForRetry),
      stillPending.map {
        case (tp, p) =>
          s"${tp.topic}#${tp.partition}" -> Pending(p.records.size, p.lastAttempt, p.attempts)
      },
      attributes
    )

  private def countsByPartition(records: Seq[ConsumerRecord[_, _]]) =
    records.groupBy(r => s"${r.topic}#${r.partition}").mapValues(_.size).toMap

  private def countsByPartition(records: Map[TopicPartition, Seq[Record]]) =
    records.map { case (tp, p) => s"${tp.topic}#${tp.partition}" -> p.size }

  case class HandleAttemptFailed(
    group: Group,
    clientId: ClientId,
    topic: String,
    partition: Partition,
    records: Seq[Record],
    willRetry: Boolean,
    attributes: Map[String, String]
  ) extends BatchEventLoopMetric
}

private[greyhound] object BatchEventLoopState {

  case class PauseResume(toPause: Set[TopicPartition], toResume: Set[TopicPartition])

  def make(implicit trace: Trace) = for {
    running   <- TRef.make(true).commit
    shutdown  <- Ref.make(false)
    offsets   <- Offsets.make
    leftovers <- Ref.make(Map.empty[TopicPartition, PendingRecords])
    paused    <- Ref.make(Set.empty[TopicPartition])
  } yield new BatchEventLoopState(running, shutdown, leftovers, paused, offsets)

  case class PendingRecords(records: Seq[Record], lastAttempt: Option[Instant] = None, attempts: Int = 0) {
    def attemptedAt(instant: Instant) = copy(lastAttempt = Some(instant), attempts = attempts + 1)

    def isEmpty = records.isEmpty

    def size = records.size

    def ++(more: Iterable[Record]) = copy(records = records ++ more)
  }
}

private[greyhound] class BatchEventLoopState(
  running: TRef[Boolean],
  shutdownRef: Ref[Boolean],
  pendingRecordsRef: Ref[Map[TopicPartition, PendingRecords]],
  pausedPartitionsRef: Ref[Set[TopicPartition]],
  val offsets: Offsets
) {
  private[greyhound] def clearPending(partitions: Set[TopicPartition])(implicit trace: Trace): UIO[Unit] = pendingRecordsRef.update {
    pending => partitions.foldLeft(pending) { case (res, tp) => res - tp }
  }

  def partitionsRevoked(partitions: Set[TopicPartition])(implicit trace: Trace) = {
    clearPending(partitions) *> pausedPartitionsRef.update(_ -- partitions)
  }

  def markHandled(partition: TopicPartition, records: Seq[Record])(implicit trace: Trace): UIO[Unit] = {
    clearPending(Set(partition)) *> offsets.update(records)
  }

  private[greyhound] def attemptFailed(partition: TopicPartition, newPending: Seq[Record])(implicit trace: Trace): URIO[Any, Unit] = {
    currentDateTime.flatMap { now =>
      pendingRecordsRef.update { current =>
        current.get(partition) match {
          case None          => current + (partition -> PendingRecords(newPending, Some(now.toInstant), 1))
          case Some(records) => current + (partition -> records.attemptedAt(now.toInstant).copy(records = newPending))
        }
      }
    }
  }

  def allPending()(implicit trace: Trace) = pendingRecordsRef.get.map(_.filterNot(_._2.isEmpty))

  def readyForRetryAndBlocked(
    backoff: Duration
  )(implicit trace: Trace): ZIO[Any, Nothing, (Map[TopicPartition, Seq[Record]], Map[TopicPartition, PendingRecords])] =
    currentDateTime.flatMap { now =>
      allPending
        .map(_.partition {
          case (_, records) =>
            records.lastAttempt.exists(_.plus(backoff).isBefore(now.toInstant))
        })
        .map {
          case (ready, notReady) =>
            ready.mapValues(_.records).toMap -> notReady
        }
    }

  def isRunning()(implicit trace: Trace) = running.get.commit

  def awaitRunning()(implicit trace: Trace) = STM.atomically {
    for {
      v <- running.get
      _ <- STM.check(v)
    } yield ()
  }

  def notShutdown()(implicit trace: Trace) = shutdownRef.get.map(!_)

  def pause()(implicit trace: Trace) = running.set(false).commit

  def resume()(implicit trace: Trace) = running.set(true).commit

  def shutdown()(implicit trace: Trace) = shutdownRef.set(true)

  def eventLoopState()(implicit trace: Trace) = for {
    rn      <- isRunning
    sd      <- shutdownRef.get
    pending <- allPending.map(_.mapValues(_.size).toMap)
    paused  <- pausedPartitions
  } yield EventLoopExposedState(rn, sd, pending, paused)

  def pausedPartitions()(implicit trace: Trace) = pausedPartitionsRef.get

  def partitionsPaused(partitions: Set[TopicPartition])(implicit trace: Trace) = pausedPartitionsRef.update(_ ++ partitions)

  def partitionsResumed(partitions: Set[TopicPartition])(implicit trace: Trace) = pausedPartitionsRef.update(_ -- partitions)

  def partitionsPausedResumed(pauseResume: PauseResume)(implicit trace: Trace) =
    partitionsPaused(pauseResume.toPause) *> partitionsResumed(pauseResume.toResume)

  def shouldPauseAndResume[R]()(implicit trace: Trace): URIO[R, PauseResume] = for {
    pending <- allPending.map(_.keySet)
    paused  <- pausedPartitions
    toPause  = pending -- paused
    toResume = paused -- pending
  } yield PauseResume(toPause, toResume)

  def appendPending(records: Consumer.Records)(implicit trace: Trace): UIO[Unit] = {
    pendingRecordsRef.update(pending =>
      records.groupBy(_.topicPartition).foldLeft(pending) {
        case (acc, (tp, records)) =>
          acc + (tp -> (acc.getOrElse(tp, PendingRecords(Nil)) ++ records))
      }
    )
  }
}
