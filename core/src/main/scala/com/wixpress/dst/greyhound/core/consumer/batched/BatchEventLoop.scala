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
import org.joda.time.DateTime
import zio.clock.Clock
import zio.duration._
import zio.stm.{STM, TRef}
import zio.{Cause, Chunk, Exit, Fiber, Has, Promise, RManaged, Ref, UIO, URIO, ZEnv, ZIO}

import scala.reflect.ClassTag

trait BatchEventLoop[R] extends Resource[R] {
  def state: UIO[EventLoopExposedState]

  def rebalanceListener: RebalanceListener[Any]

  def requestSeek(toOffsets: Map[TopicPartition, Offset]): UIO[Unit]
}

private[greyhound] class BatchEventLoopImpl[R <: Has[_] : ClassTag](group: Group,
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

  private val consumerAttributes = consumer.config.consumerAttributes

  override def state: UIO[EventLoopExposedState] = elState.eventLoopState

  private def report(metric: GreyhoundMetric) = GreyhoundMetrics.report(metric).provide(capturedR)

  override def pause: URIO[R, Unit] =
    report(PausingEventLoop(clientId, group, consumerAttributes)) *> elState.pause()

  override def resume: URIO[R, Unit] =
    report(ResumingEventLoop(clientId, group, consumerAttributes)) *> elState.resume()

  override def isAlive: UIO[Boolean] =
    loopFiberRef.get.flatMap(_.fold(UIO(false))(_.poll.map {
      case Some(Exit.Failure(_)) => false
      case _ => true
    }))

  def startPolling(): URIO[R with ZEnv with GreyhoundMetrics, Unit] = for {
    _ <- report(StartingEventLoop(clientId, group, consumerAttributes))
    fiber <- pollOnce()
      .sandbox.ignore
      .repeatWhileM(_ => elState.notShutdown).forkDaemon
    _ <- loopFiberRef.set(Some(fiber))
  } yield ()

  private[greyhound] def shutDown(): UIO[Unit] = for {
    _ <- report(StoppingEventLoop(clientId, group, consumerAttributes))
    _ <- elState.shutdown()
  } yield ()

  private def pollOnce(): URIO[R with ZEnv with GreyhoundMetrics, Unit] =
    elState.awaitRunning.timed.provide(capturedR).flatMap { case (waited, _) =>
      for {
        _ <- ZIO.when(waited > 1.second)(report(EventloopWaitedForResume(group, clientId, waited, consumerAttributes)))
        _ <- pollAndHandle()
        _ <- seekIfRequested()
        _ <- commitOffsets()
      } yield ()
    }

  private def seekIfRequested() =
    seekRequests.get
      .flatMap(offsetSeeks =>
        ZIO.when(offsetSeeks.nonEmpty)(
            consumer.seek(offsetSeeks)
              .tap(_ => seekRequests.set(Map.empty))))

  private def pollAndHandle(): URIO[R with Clock, Unit] = for {
    _ <- pauseAndResume().provide(capturedR)
    records <- consumer.poll(config.pollTimeout).provide(capturedR).catchAll(_ => UIO(Nil))
      .flatMap(records => seekRequests.get.map(seeks => records.filterNot(record => seeks.keys.toSet.contains(record.topicPartition))))
    _ <- handleRecords(records)
      .timed
      .tap { case (duration, _) => report(FullBatchHandled(clientId, group, records.toSeq, duration, consumerAttributes)) }
  } yield ()

  private def pauseAndResume() = for {
    pr <- elState.shouldPauseAndResume()
    _ <- ZIO.when(pr.toPause.nonEmpty)((consumer.pause(pr.toPause) *> elState.partitionsPaused(pr.toPause)).ignore)
    _ <- ZIO.when(pr.toResume.nonEmpty)((consumer.resume(pr.toResume) *> elState.partitionsResumed(pr.toResume)).ignore)
  } yield ()


  private def handleRecords(polled: Records): ZIO[R, Nothing, Unit] = {
    retry match {
      case None => handleWithNoRetry(polled)
      case Some(retry) => handleWithRetry(polled, retry.backoff)
    }
  }

  private def handleWithRetry(polled: Records, retryBackoff: Duration): ZIO[R, Nothing, Unit] = {
    for {
      forRetryPairs <- elState.readyForRetryAndBlocked(retryBackoff).provide(capturedR)
      (forRetry, stillPending) = forRetryPairs
      blockedPartitions = stillPending.keySet
      (polledReadyToGo, polledOnBlocked) = polled.partition(r => !blockedPartitions(r.topicPartition))
      polledByPartition = recordsByPartition(polledReadyToGo)
      // we shouldn't get polled records for partitions with pending records, as they should be paused
      // but if we do for some reason - we append them to the pending records
      _ <- elState.appendPending(polledOnBlocked)
      // some for partitions ready for retry
      readyToHandle = concatByKey(forRetry, polledByPartition)
      _ <- report(EventLoopIteration(group, clientId, polled.toSeq, forRetry, stillPending, consumerAttributes))
      _ <- handleInParallelWithRetry(readyToHandle)
    } yield ()
  }

  private def handleInParallelWithRetry(readyToHandle: Map[TopicPartition, Seq[Record]]): ZIO[R, Nothing, Unit] = {
    ZIO.foreachParN_(config.parallelism)(readyToHandle) { case (tp, records) =>
      (handler.handle(ConsumerRecordBatch(tp.topic, tp.partition, records)) *>
        elState.markHandled(tp, records)).catchAllCause { cause =>
        val forceNoRetry = cause.failureOption.fold(false)(_.forceNoRetry)
        report(HandleAttemptFailed(group, clientId, tp.topic, tp.partition, records, !forceNoRetry, consumerAttributes)) *> {
          if (forceNoRetry) elState.markHandled(tp, records)
          else elState.attemptFailed(tp, records).provide(capturedR)
        }
      }
    }
  }

  private def concatByKey[K, V](a: Map[K, Seq[V]],
                                b: Map[K, Seq[V]]) = {
    b.foldLeft(a) { case (acc, (tp, recs)) =>
      acc + (tp -> (acc.getOrElse(tp, Nil) ++ recs))
    }
  }

  private def handleWithNoRetry(polled: Records) = {
    ZIO.foreachParN_(config.parallelism)(recordsByPartition(polled)) { case (tp, records) =>
      handler.handle(ConsumerRecordBatch(tp.topic, tp.partition, records))
        .sandbox.ignore *>
        elState.offsets.update(records)
    }
  }

  private def commitOffsets(): UIO[Unit] =
    elState.offsets.committable.flatMap { committable =>
      consumer.commit(committable).provide(capturedR).catchAllCause { _ => elState.offsets.update(committable) }
    }

  private def recordsByPartition(records: Records): Map[TopicPartition, Seq[Record]] = {
    records.toSeq.groupBy(r => TopicPartition(r.topic, r.partition))
  }

  override def requestSeek(toOffsets: Map[TopicPartition, Offset]): UIO[Unit] =
    seekRequests.update(_ ++ toOffsets).tap(_ => ZIO.debug(s"${DateTime.now.toString()} &&&&&&& requested seek on ${toOffsets}"))
}

object BatchEventLoop {
  type Handler[-R, +E] = BatchRecordHandler[R, E, Chunk[Byte], Chunk[Byte]]
  type Record = ConsumerRecord[Chunk[Byte], Chunk[Byte]]

  type ExtraEnv = GreyhoundMetrics with ZEnv

  def make[R <: Has[_] : ClassTag](group: Group,
                                   initialSubscription: ConsumerSubscription,
                                   consumer: Consumer,
                                   handler: Handler[R, Any],
                                   clientId: ClientId,
                                   retry: Option[BatchRetryConfig] = None,
                                   config: BatchEventLoopConfig = BatchEventLoopConfig.Default): RManaged[R with ExtraEnv, BatchEventLoop[R]] = {
    val start = for {
      state <- BatchEventLoopState.make
      partitionsAssigned <- Promise.make[Nothing, Unit]
      rebalanceListener = listener(state, config, partitionsAssigned)
      _ <- subscribe(initialSubscription, rebalanceListener)(consumer)
      fiberRef <- Ref.make(Option.empty[Fiber[_, _]])
      instrumentedHandler <- handlerWithMetrics(group, clientId, handler, consumer.config.consumerAttributes)
      env <- ZIO.environment[ExtraEnv]
      _ <- ZIO.when(config.startPaused)(state.pause())
      seekRequests <- Ref.make(Map.empty[TopicPartition, Offset])
      eventLoop = new BatchEventLoopImpl[R](group, clientId, consumer, instrumentedHandler, retry, config, state, rebalanceListener, fiberRef, env, seekRequests)
      _ <- eventLoop.startPolling()
      _ <- ZIO.when(!config.startPaused)(partitionsAssigned.await)
    } yield eventLoop

    start.toManaged(_.shutDown())
  }

  private def listener(state: BatchEventLoopState,
                       config: BatchEventLoopConfig,
                       partitionsAssigned: Promise[Nothing, Unit]): RebalanceListener[Any] = {
    config.rebalanceListener *>
      new RebalanceListener[Any] {
        override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition]): URIO[Any, DelayedRebalanceEffect] = {
          state.partitionsRevoked(partitions).as(DelayedRebalanceEffect.unit)
        }

        override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): UIO[Any] =
          partitionsAssigned.succeed(())
      }
  }

  private def handlerWithMetrics[R <: Has[_] : ClassTag, E](group: Group, clientId: ClientId, handler: Handler[R, E], consumerAttributes: Map[String, String]): ZIO[ExtraEnv, Nothing, BatchRecordHandler[R, E, Chunk[Byte], Chunk[Byte]]] =
    ZIO.environment[ExtraEnv].map { env =>
      def report(m: GreyhoundMetric) = GreyhoundMetrics.report(m).provide(env)
      def currentTime = zio.clock.currentDateTime.orDie.map(_.toInstant.toEpochMilli).provide(env)
      val nanoTime = env.get[Clock.Service].nanoTime
      new Handler[R, E] {
        override def handle(records: ConsumerRecordBatch[Chunk[Byte], Chunk[Byte]]): ZIO[R, HandleError[E], Any] = {
          val effect = for {
            now <- currentTime
            sinceProduce = now - records.records.headOption.fold(now)(_.producedTimestamp)
            sincePoll = now - records.records.headOption.fold(now)(_.pollTime)
            _ <- report(HandlingRecords(records.topic, group, records.partition, clientId, records.records, sinceProduce, sincePoll, consumerAttributes)) *>
              handler.handle(records).timedWith(nanoTime).flatMap {
                case (duration, _) =>
                  report(RecordsHandled(records.topic, group, records.partition, clientId, records.records, duration, consumerAttributes))
              }
          } yield ()
          effect
        }
      }
    }
}

case class EventLoopExposedState(running: Boolean, shutdown: Boolean, pendingRecords: Map[TopicPartition, Int], pausedPartitions: Set[TopicPartition]) {
  def paused = !running
}

case class BatchEventLoopConfig(pollTimeout: Duration,
                                rebalanceListener: RebalanceListener[Any],
                                parallelism: Int = 8,
                                startPaused: Boolean
                               ) {
  def withParallelism(n: Int) = copy(parallelism = n)
}

object BatchEventLoopConfig {
  val Default = BatchEventLoopConfig(
    pollTimeout = 500.millis,
    rebalanceListener = RebalanceListener.Empty,
    startPaused = false)
}

sealed trait BatchEventLoopMetric extends GreyhoundMetric

object BatchEventLoopMetric {
  case class FullBatchHandled[K, V](clientId: ClientId, group: Group, records: Seq[ConsumerRecord[K, V]], duration: Duration, attributes: Map[String, String]) extends BatchEventLoopMetric

  case class EventLoopFailedToSeek(seekRequests: Map[TopicPartition, Offset], t: IllegalStateException) extends BatchEventLoopMetric

  case class RecordsHandled[K, V](topic: String, group: Group, partition: Partition, clientId: ClientId, records: Seq[ConsumerRecord[K, V]], duration: Duration, attributes: Map[String, String]) extends BatchEventLoopMetric

  case class HandlingRecords[K, V](topic: String, group: Group, partition: Partition, clientId: ClientId, records: Seq[ConsumerRecord[K, V]], timeSinceProduceMs: Long, timeSincePollMs: Long, attributes: Map[String, String]) extends BatchEventLoopMetric

  case class RecordsPendingForRetry[K, V](topic: String, group: Group, partition: Int, clientId: ClientId, records: Seq[ConsumerRecord[K, V]], attributes: Map[String, String]) extends BatchEventLoopMetric

  case class EventloopWaitedForResume(group: Group, clientId: ClientId, waitedFor: Duration, attributes: Map[String, String]) extends BatchEventLoopMetric

  case class Pending(count: Int, lastAttempt: Option[Instant], attempts: Int)

  case class EventLoopIteration(group: Group, clientId: ClientId, polled: Map[String, Int], readyForRetry: Map[String, Int], stillPending: Map[String, Pending], attributes: Map[String, String]) extends BatchEventLoopMetric

  def EventLoopIteration(group: Group, clientId: ClientId, polled: Seq[Record], readyForRetry: Map[TopicPartition, Seq[Record]], stillPending: Map[TopicPartition, PendingRecords], attributes: Map[String, String]): EventLoopIteration =
    EventLoopIteration(group, clientId, countsByPartition(polled), countsByPartition(readyForRetry),
      stillPending.map { case (tp, p) =>
        s"${tp.topic}#${tp.partition}" -> Pending(p.records.size, p.lastAttempt, p.attempts)
      },
      attributes
    )

  private def countsByPartition(records: Seq[ConsumerRecord[_, _]]) =
    records.groupBy(r => s"${r.topic}#${r.partition}").mapValues(_.size)

  private def countsByPartition(records: Map[TopicPartition, Seq[Record]]) =
    records.map { case (tp, p) => s"${tp.topic}#${tp.partition}" -> p.size }

  case class HandleAttemptFailed(group: Group, clientId: ClientId, topic: String, partition: Partition, records: Seq[Record], willRetry: Boolean, attributes: Map[String, String]) extends BatchEventLoopMetric
}

private[greyhound] object BatchEventLoopState {

  case class PauseResume(toPause: Set[TopicPartition], toResume: Set[TopicPartition])

  def make = for {
    running <- TRef.make(true).commit
    shutdown <- Ref.make(false)
    offsets <- Offsets.make
    leftovers <- Ref.make(Map.empty[TopicPartition, PendingRecords])
    paused <- Ref.make(Set.empty[TopicPartition])
  } yield new BatchEventLoopState(running, shutdown, leftovers, paused, offsets)

  case class PendingRecords(records: Seq[Record], lastAttempt: Option[Instant] = None, attempts: Int = 0) {
    def attemptedAt(instant: Instant) = copy(lastAttempt = Some(instant), attempts = attempts + 1)

    def isEmpty = records.isEmpty

    def size = records.size

    def ++(more: Iterable[Record]) = copy(records = records ++ more)
  }
}

private[greyhound] class BatchEventLoopState(running: TRef[Boolean],
                                             shutdownRef: Ref[Boolean],
                                             pendingRecordsRef: Ref[Map[TopicPartition, PendingRecords]],
                                             pausedPartitionsRef: Ref[Set[TopicPartition]],
                                             val offsets: Offsets) {
  private[greyhound] def clearPending(partitions: Set[TopicPartition]): UIO[Unit] = pendingRecordsRef.update { pending =>
    partitions.foldLeft(pending) { case (res, tp) => res - tp }
  }

  def partitionsRevoked(partitions: Set[TopicPartition]) = {
    clearPending(partitions) *>
      pausedPartitionsRef.update(_ -- partitions)
  }

  def markHandled(partition: TopicPartition, records: Seq[Record]): UIO[Unit] = {
    clearPending(Set(partition)) *>
      offsets.update(records)
  }

  private[greyhound] def attemptFailed(partition: TopicPartition, newPending: Seq[Record]): URIO[Clock, Unit] = {
    zio.clock.currentDateTime.orDie.flatMap { now =>
      pendingRecordsRef.update { current =>
        current.get(partition) match {
          case None => current + (partition -> PendingRecords(newPending, Some(now.toInstant), 1))
          case Some(records) => current + (partition -> records.attemptedAt(now.toInstant).copy(records = newPending))
        }
      }
    }
  }

  def allPending = pendingRecordsRef.get.map(_.filterNot(_._2.isEmpty))

  def readyForRetryAndBlocked(backoff: Duration): ZIO[Clock, Nothing, (Map[TopicPartition, Seq[Record]], Map[TopicPartition, PendingRecords])] =
    zio.clock.currentDateTime.orDie.flatMap { now =>
      allPending.map(
        _.partition { case (_, records) =>
          records.lastAttempt.exists(_.plus(backoff).isBefore(now.toInstant))
        }).map { case (ready, notReady) =>
        ready.mapValues(_.records) -> notReady
      }
    }

  def isRunning = running.get.commit

  def awaitRunning = STM.atomically {
    for {
      v <- running.get
      _ <- STM.check(v)
    } yield ()
  }

  def notShutdown = shutdownRef.get.map(!_)

  def pause() = running.set(false).commit

  def resume() = running.set(true).commit

  def shutdown() = shutdownRef.set(true)

  def eventLoopState = for {
    rn <- isRunning
    sd <- shutdownRef.get
    pending <- allPending.map(_.mapValues(_.size))
    paused <- pausedPartitions
  } yield EventLoopExposedState(rn, sd, pending, paused)

  def pausedPartitions = pausedPartitionsRef.get

  def partitionsPaused(partitions: Set[TopicPartition]) = pausedPartitionsRef.update(_ ++ partitions)

  def partitionsResumed(partitions: Set[TopicPartition]) = pausedPartitionsRef.update(_ -- partitions)

  def partitionsPausedResumed(pauseResume: PauseResume) =
    partitionsPaused(pauseResume.toPause) *> partitionsResumed(pauseResume.toResume)

  def shouldPauseAndResume[R](): URIO[R, PauseResume] = for {
    pending <- allPending
    paused <- pausedPartitions
    toPause = pending.keySet -- paused
    toResume = paused -- pending.keySet
  } yield PauseResume(toPause, toResume)


  def appendPending(records: Consumer.Records): UIO[Unit] = {
    pendingRecordsRef.update(
      pending =>
        records.groupBy(_.topicPartition).foldLeft(pending) { case (acc, (tp, records)) =>
          acc + (tp -> (acc.getOrElse(tp, PendingRecords(Nil)) ++ records))
        }
    )
  }
}
