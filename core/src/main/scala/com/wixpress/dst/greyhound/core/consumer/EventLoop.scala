package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.EventLoopMetric._
import com.wixpress.dst.greyhound.core.consumer.EventLoopState.{Paused, Running, ShuttingDown}
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.{report, trace}
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown.ShutdownPromise
import zio._
import zio.Clock

trait EventLoop[-R] extends Resource[R] {
  self =>
  def state: UIO[EventLoopExposedState]

  def waitForCurrentRecordsCompletion: URIO[Any, Unit]

  def rebalanceListener: RebalanceListener[Any]

  def stop: URIO[GreyhoundMetrics, Any]
}

object EventLoop {
  type Handler[-R] = RecordHandler[R, Nothing, Chunk[Byte], Chunk[Byte]]

  def make[R](
    group: Group,
    initialSubscription: ConsumerSubscription,
    consumer: Consumer,
    handler: Handler[R],
    clientId: ClientId,
    config: EventLoopConfig = EventLoopConfig.Default,
    consumerAttributes: Map[String, String] = Map.empty,
    workersShutdownRef: Ref[Map[TopicPartition, ShutdownPromise]]
  )(implicit trace: Trace): RIO[R with Env, EventLoop[GreyhoundMetrics]] = {
    val start = for {
      _                   <- report(StartingEventLoop(clientId, group, consumerAttributes))
      offsets             <- Offsets.make
      handle               = handler.andThen(offsets.update).handle(_)
      dispatcher          <- Dispatcher.make(
                               group,
                               clientId,
                               handle,
                               config.lowWatermark,
                               config.highWatermark,
                               config.drainTimeout,
                               config.delayResumeOfPausedPartition,
                               consumerAttributes,
                               workersShutdownRef,
                               config.startPaused
                             )
      positionsRef        <- Ref.make(Map.empty[TopicPartition, Offset])
      pausedPartitionsRef <- Ref.make(Set.empty[TopicPartition])
      partitionsAssigned  <- Promise.make[Nothing, Unit]
      // TODO how to handle errors in subscribe?
      rebalanceListener    = listener(pausedPartitionsRef, config, dispatcher, partitionsAssigned, group, consumer, clientId, offsets)
      _                   <- subscribe(initialSubscription, rebalanceListener)(consumer)
      running             <- Ref.make[EventLoopState](Running)
      fiber               <- pollOnce(running, consumer, dispatcher, pausedPartitionsRef, positionsRef, offsets, config, clientId, group)
                               .repeatWhile(_ == true)
                               .forkDaemon
      _                   <- partitionsAssigned.await
      env                 <- ZIO.environment[Env]
    } yield (dispatcher, fiber, offsets, positionsRef, running, rebalanceListener.provideEnvironment(env))

    start
      .map {
        case (dispatcher, fiber, offsets, positionsRef, running, listener) =>
          new EventLoop[GreyhoundMetrics] {

            override def stop: URIO[GreyhoundMetrics, Any] =
              stopLoop(group, consumer, clientId, consumerAttributes, config, running, fiber, offsets, dispatcher)

            override def pause(implicit trace: Trace): URIO[GreyhoundMetrics, Unit] =
              (report(PausingEventLoop(clientId, group, consumerAttributes)) *> running.set(Paused) *> dispatcher.pause).unit

            override def resume(implicit trace: Trace): URIO[GreyhoundMetrics, Unit] =
              (report(ResumingEventLoop(clientId, group, consumerAttributes)) *> running.set(Running) *> dispatcher.resume).unit

            override def isAlive(implicit trace: Trace): UIO[Boolean] = fiber.poll.map {
              case Some(Exit.Failure(_)) => false
              case _                     => true
            }

            override def state: UIO[EventLoopExposedState] = (positionsRef.get zip dispatcher.expose)
              .map {
                case (positions, dispatcherState) =>
                  EventLoopExposedState(positions, dispatcherState)
              }

            override def rebalanceListener: RebalanceListener[Any] = listener

            override def waitForCurrentRecordsCompletion: URIO[Any, Unit] = dispatcher.waitForCurrentRecordsCompletion
          }
      }
  }

  private def stopLoop[R](
    group: Group,
    consumer: Consumer,
    clientId: ClientId,
    consumerAttributes: Map[String, String],
    config: EventLoopConfig,
    running: Ref[EventLoopState],
    fiber: Fiber.Runtime[Nothing, Boolean],
    offsets: Offsets,
    dispatcher: Dispatcher[R]
  ) =
    for {
      _       <- report(StoppingEventLoop(clientId, group, consumerAttributes))
      _       <- running.set(ShuttingDown)
      drained <- (fiber.join *> dispatcher.shutdown).timeout(config.drainTimeout)
      _       <- ZIO.when(drained.isEmpty)(report(DrainTimeoutExceeded(clientId, group, config.drainTimeout.toMillis, consumerAttributes)))
      _       <- commitOffsets(consumer, offsets)
    } yield ()

  private def updatePositions(
    records: Consumer.Records,
    positionsRef: Ref[Map[TopicPartition, Offset]],
    consumer: Consumer,
    clientId: ClientId
  )(implicit trace: Trace) =
    ZIO
      .foreach(records.map(_.topicPartition))(tp => consumer.position(tp).flatMap(offset => positionsRef.update(_ + (tp -> offset))))
      .catchAll(t => report(FailedToUpdatePositions(t, clientId, consumer.config.consumerAttributes)))

  private def pollOnce[R2](
    running: Ref[EventLoopState],
    consumer: Consumer,
    dispatcher: Dispatcher[R2],
    paused: Ref[Set[TopicPartition]],
    positionsRef: Ref[Map[TopicPartition, Offset]],
    offsets: Offsets,
    config: EventLoopConfig,
    clientId: ClientId,
    group: Group
  ): URIO[R2 with Env, Boolean] =
    running.get.flatMap {
      case Running =>
        for {
          _       <- resumePartitions(consumer, clientId, group, dispatcher, paused)
          records <- pollAndHandle(consumer, dispatcher, paused, config)
          _       <- updatePositions(records, positionsRef, consumer, clientId)
          _       <- commitOffsets(consumer, offsets)
          _       <- ZIO.when(records.isEmpty)(ZIO.sleep(50.millis))
        } yield true

      case ShuttingDown => ZIO.succeed(false)
      case Paused       => ZIO.sleep(100.millis).as(true)
    }

  private def listener(
    pausedPartitionsRef: Ref[Set[TopicPartition]],
    config: EventLoopConfig,
    dispatcher: Dispatcher[_],
    partitionsAssigned: Promise[Nothing, Unit],
    group: Group,
    consumer0: Consumer,
    clientId: ClientId,
    offsets: Offsets
  ) = {
    config.rebalanceListener *>
      new RebalanceListener[GreyhoundMetrics] {
        override def onPartitionsRevoked(
          consumer: Consumer,
          partitions: Set[TopicPartition]
        )(implicit trace: Trace): URIO[GreyhoundMetrics, DelayedRebalanceEffect] = {
          pausedPartitionsRef.set(Set.empty) *>
            dispatcher.revoke(partitions).timeout(config.drainTimeout).flatMap { drained =>
              ZIO.when(drained.isEmpty)(
                report(DrainTimeoutExceeded(clientId, group, config.drainTimeout.toMillis, consumer.config.consumerAttributes))
              )
            } *> commitOffsetsOnRebalance(consumer0, offsets)
        }

        override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(implicit trace: Trace): UIO[Any] =
          partitionsAssigned.succeed(())
      }
  }

  private def resumePartitions[R1, R2](
    consumer: Consumer,
    clientId: ClientId,
    group: Group,
    dispatcher: Dispatcher[R2],
    pausedRef: Ref[Set[TopicPartition]]
  ) =
    for {
      paused             <- pausedRef.get
      partitionsToResume <- dispatcher.resumeablePartitions(paused)
      _                  <- ZIO.when(partitionsToResume.nonEmpty)(
                              report(LowWatermarkReached(clientId, group, partitionsToResume, consumer.config.consumerAttributes))
                            )
      _                  <- consumer
                              .resume(partitionsToResume)
                              .tapError(e => ZIO.succeed(e.printStackTrace()))
                              .ignore
      _                  <- pausedRef.update(_ -- partitionsToResume)
    } yield ()

  private def pollAndHandle[R1, R2](
    consumer: Consumer,
    dispatcher: Dispatcher[R2],
    pausedRef: Ref[Set[TopicPartition]],
    config: EventLoopConfig
  ) =
    for {
      records      <- consumer.poll(config.pollTimeout).catchAll(_ => ZIO.succeed(Nil))
      paused       <- pausedRef.get
      pausedTopics <- ZIO.foldLeft(records)(paused) { (acc, record) =>
                        val partition = record.topicPartition
                        if (acc contains partition)
                          report(PartitionThrottled(partition, record.offset, consumer.config.consumerAttributes)).as(acc)
                        else
                          dispatcher.submit(record).flatMap {
                            case SubmitResult.Submitted => ZIO.succeed(acc)
                            case SubmitResult.Rejected  =>
                              report(HighWatermarkReached(partition, record.offset, consumer.config.consumerAttributes)) *>
                                consumer.pause(record).fold(_ => acc, _ => acc + partition)
                          }
                      }
      _            <- pausedRef.update(_ => pausedTopics)
    } yield records

  private def commitOffsets(consumer: Consumer, offsets: Offsets): URIO[GreyhoundMetrics, Unit] =
    offsets.committable.flatMap { committable => consumer.commit(committable).catchAll { _ => offsets.update(committable) } }

  private def commitOffsetsOnRebalance(
    consumer: Consumer,
    offsets: Offsets
  ): URIO[GreyhoundMetrics, DelayedRebalanceEffect] = {
    for {
      committable <- offsets.committable
      tle         <- consumer.commitOnRebalance(committable).catchAll { _ => offsets.update(committable) *> DelayedRebalanceEffect.zioUnit }
      runtime     <- ZIO.runtime[Any]
    } yield tle.catchAll { _ =>
      zio.Unsafe.unsafe { implicit s =>
        runtime.unsafe
          .run(
            offsets.update(committable)
          )
          .getOrThrowFiberFailure()
      }
    }
  }

}

case class EventLoopConfig(
  pollTimeout: Duration,
  drainTimeout: Duration,
  lowWatermark: Int,
  highWatermark: Int,
  rebalanceListener: RebalanceListener[Any],
  delayResumeOfPausedPartition: Long,
  startPaused: Boolean
) // TODO parametrize?

object EventLoopConfig {
  val Default = EventLoopConfig(
    pollTimeout = 500.millis,
    drainTimeout = 30.seconds,
    lowWatermark = 128,
    highWatermark = 256,
    rebalanceListener = RebalanceListener.Empty,
    delayResumeOfPausedPartition = 0,
    startPaused = false
  )
}

sealed trait EventLoopMetric extends GreyhoundMetric

object EventLoopMetric {

  case class StartingEventLoop(clientId: ClientId, group: Group, attributes: Map[String, String] = Map.empty) extends EventLoopMetric

  case class PausingEventLoop(clientId: ClientId, group: Group, attributes: Map[String, String] = Map.empty) extends EventLoopMetric

  case class ResumingEventLoop(clientId: ClientId, group: Group, attributes: Map[String, String] = Map.empty) extends EventLoopMetric

  case class StoppingEventLoop(clientId: ClientId, group: Group, attributes: Map[String, String] = Map.empty) extends EventLoopMetric

  case class DrainTimeoutExceeded(clientId: ClientId, group: Group, timeoutMs: Long, attributes: Map[String, String] = Map.empty)
      extends EventLoopMetric

  case class HighWatermarkReached(partition: TopicPartition, onOffset: Offset, attributes: Map[String, String] = Map.empty)
      extends EventLoopMetric

  case class PartitionThrottled(partition: TopicPartition, onOffset: Offset, attributes: Map[String, String] = Map.empty)
      extends EventLoopMetric

  case class LowWatermarkReached(
    clientId: ClientId,
    group: Group,
    partitionsToResume: Set[TopicPartition],
    attributes: Map[String, String] = Map.empty
  ) extends EventLoopMetric

  case class FailedToUpdatePositions(t: Throwable, clientId: ClientId, attributes: Map[String, String] = Map.empty) extends EventLoopMetric

}

sealed trait EventLoopState

object EventLoopState {

  case object Running extends EventLoopState

  case object Paused extends EventLoopState

  case object ShuttingDown extends EventLoopState

}

case class EventLoopExposedState(latestOffsets: Map[TopicPartition, Offset], dispatcherState: DispatcherExposedState) {
  def withDispatcherState(state: Dispatcher.DispatcherState) =
    copy(dispatcherState = dispatcherState.copy(state = state))

  def topics = dispatcherState.topics
}
