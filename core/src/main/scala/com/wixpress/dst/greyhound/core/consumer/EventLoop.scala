package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.EventLoopMetric._
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler, TopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import org.apache.kafka.clients.consumer.ConsumerRecords
import zio._
import zio.blocking.Blocking
import zio.duration._

import scala.collection.JavaConverters._

trait EventLoop[-R] extends Resource[R] {
  self =>
  def state: UIO[DispatcherExposedState]
  def rebalanceListener: RebalanceListener[Any]
}

object EventLoop {
  type Handler[-R] = RecordHandler[R, Nothing, Chunk[Byte], Chunk[Byte]]

  def make[R](group: Group,
              initialSubscription: ConsumerSubscription,
              consumer: Consumer,
              handler: Handler[R],
              clientId: ClientId,
              config: EventLoopConfig = EventLoopConfig.Default): RManaged[R with Env, EventLoop[GreyhoundMetrics with Blocking]] = {
    val start = for {
      _ <- report(StartingEventLoop(clientId, group))
      offsets <- Offsets.make
      handle = handler.andThen(offsets.update).handle(_)
      dispatcher <- Dispatcher.make(group, clientId, handle, config.lowWatermark, config.highWatermark)
      pausedPartitionsRef <- Ref.make(Set.empty[TopicPartition])
      partitionsAssigned <- Promise.make[Nothing, Unit]
      // TODO how to handle errors in subscribe?
      rebalanceListener = listener(pausedPartitionsRef, config, dispatcher, partitionsAssigned, group, consumer, clientId, offsets)
      _ <- subscribe(initialSubscription, rebalanceListener)(consumer)
      running <- Ref.make(true)
      fiber <- pollOnce(running, consumer, dispatcher, pausedPartitionsRef, offsets, config, clientId, group)
        .doWhile(_ == true).forkDaemon
      _ <- partitionsAssigned.await
      env <-  ZIO.environment[R with Env]
    } yield (dispatcher, fiber, offsets, running, rebalanceListener.provide(env))

    start.toManaged {
      case (dispatcher, fiber, offsets, running, _) => for {
        _ <- report(StoppingEventLoop(clientId, group))
        _ <- running.set(false)
        drained <- (fiber.join *> dispatcher.shutdown).timeout(config.drainTimeout)
        _ <- ZIO.when(drained.isEmpty)(report(DrainTimeoutExceeded(clientId, group)))
        _ <- commitOffsets(consumer, offsets)
      } yield ()
    }.map {
      case (dispatcher, fiber, _, _, listener) =>
        new EventLoop[GreyhoundMetrics with Blocking] {
          override def pause: URIO[GreyhoundMetrics with Blocking, Unit] =
            report(PausingEventLoop(clientId, group)) *> dispatcher.pause

          override def resume: URIO[GreyhoundMetrics with Blocking, Unit] =
            report(ResumingEventLoop(clientId, group)) *> dispatcher.resume

          override def isAlive: UIO[Boolean] = fiber.poll.map {
            case Some(Exit.Failure(_)) => false
            case _ => true
          }

          override def state: UIO[DispatcherExposedState] = dispatcher.expose

          override def rebalanceListener: RebalanceListener[Any] = listener
        }
    }
  }

  private def pollOnce[R2](running: Ref[Boolean],
                           consumer: Consumer,
                           dispatcher: Dispatcher[R2],
                           paused: Ref[Set[TopicPartition]],
                           offsets: Offsets,
                           config: EventLoopConfig,
                           clientId: ClientId,
                           group: Group): URIO[R2 with Env, Boolean] =
    running.get.flatMap {
      case true =>
        for {
          _ <- resumePartitions(consumer, clientId, group, dispatcher, paused)
          _ <- pollAndHandle(consumer, dispatcher, paused, config, clientId)
          _ <- commitOffsets(consumer, offsets)
        } yield true

      case false => UIO(false)
    }

  private def listener(pausedPartitionsRef: Ref[Set[TopicPartition]],
                               config: EventLoopConfig,
                               dispatcher: Dispatcher[_],
                               partitionsAssigned: Promise[Nothing, Unit],
                               group: Group, consumer: Consumer, clientId: ClientId, offsets: Offsets) = {
    config.rebalanceListener *>
      new RebalanceListener[ZEnv with GreyhoundMetrics] {
        override def onPartitionsRevoked(partitions: Set[TopicPartition]): URIO[ZEnv with GreyhoundMetrics, DelayedRebalanceEffect] = {
          pausedPartitionsRef.set(Set.empty) *>
           dispatcher.revoke(partitions).timeout(config.drainTimeout).flatMap { drained =>
               ZIO.when(drained.isEmpty)(report(DrainTimeoutExceeded(clientId, group)))
           } *>
           commitOffsetsOnRebalance(consumer, offsets)
        }

        override def onPartitionsAssigned(partitions: Set[TopicPartition]): UIO[Any] =
            partitionsAssigned.succeed(())
      }
  }


  private def resumePartitions[R1, R2](consumer: Consumer,
                                       clientId: ClientId,
                                       group: Group,
                                       dispatcher: Dispatcher[R2],
                                       pausedRef: Ref[Set[TopicPartition]]) =
    for {
      paused <- pausedRef.get
      partitionsToResume <- dispatcher.resumeablePartitions(paused)
      _ <- ZIO.when(partitionsToResume.nonEmpty)(
        report(LowWatermarkReached(clientId, group, partitionsToResume)))
      _ <- consumer.resume(partitionsToResume)
        .tapError(e => UIO(e.printStackTrace())).ignore
      _ <- pausedRef.update(_ -- partitionsToResume)
    } yield ()

  private val emptyRecords = ZIO.succeed(ConsumerRecords.empty())

  private def pollAndHandle[R1, R2](consumer: Consumer,
                                    dispatcher: Dispatcher[R2],
                                    pausedRef: Ref[Set[TopicPartition]],
                                    config: EventLoopConfig,
                                    clientId: ClientId) =
    consumer.poll(config.pollTimeout).catchAll(_ => emptyRecords).flatMap { records =>
      pausedRef.get.flatMap(paused =>
        ZIO.foldLeft(records.asScala)(paused) { (acc, kafkaRecord) =>
          val record = ConsumerRecord(kafkaRecord)
          val partition = TopicPartition(record)
          if (acc contains partition)
            report(PartitionThrottled(partition, record.offset)).as(acc)
          else
            dispatcher.submit(record).flatMap {
              case SubmitResult.Submitted => ZIO.succeed(acc)
              case SubmitResult.Rejected =>
                report(HighWatermarkReached(partition, record.offset)) *>
                  consumer.pause(record).fold(_ => acc, _ => acc + partition)
            }
        }.flatMap(pausedTopics => pausedRef.update(_ => pausedTopics)))
    }

  private def commitOffsets(consumer: Consumer,
                               offsets: Offsets): URIO[Blocking with GreyhoundMetrics, Unit] =
    offsets.committable.flatMap { committable =>
      consumer.commit(committable).catchAll { _ => offsets.update(committable) }
    }

  private def commitOffsetsOnRebalance(consumer: Consumer,
                                       offsets: Offsets): URIO[Blocking with GreyhoundMetrics, DelayedRebalanceEffect] = {
    for {
      committable <- offsets.committable
      tle <- consumer.commitOnRebalance(committable).catchAll  { _ => offsets.update(committable) *> DelayedRebalanceEffect.zioUnit }
      runtime <- ZIO.runtime[Any]
    } yield tle.catchAll { _ => runtime.unsafeRunTask(offsets.update(committable)) }
  }

}

case class EventLoopConfig(pollTimeout: Duration,
                           drainTimeout: Duration,
                           lowWatermark: Int,
                           highWatermark: Int,
                           rebalanceListener: RebalanceListener[Any]) // TODO parametrize?

object EventLoopConfig {
  val Default = EventLoopConfig(
    pollTimeout = 500.millis,
    drainTimeout = 30.seconds,
    lowWatermark = 128,
    highWatermark = 256,
    rebalanceListener = RebalanceListener.Empty)
}

sealed trait EventLoopMetric extends GreyhoundMetric

object EventLoopMetric {

  case class StartingEventLoop(clientId: ClientId, group: Group) extends EventLoopMetric

  case class PausingEventLoop(clientId: ClientId, group: Group) extends EventLoopMetric

  case class ResumingEventLoop(clientId: ClientId, group: Group) extends EventLoopMetric

  case class StoppingEventLoop(clientId: ClientId, group: Group) extends EventLoopMetric

  case class DrainTimeoutExceeded(clientId: ClientId, group: Group) extends EventLoopMetric

  case class HighWatermarkReached(partition: TopicPartition, onOffset: Offset) extends EventLoopMetric

  case class PartitionThrottled(partition: TopicPartition, onOffset: Offset) extends EventLoopMetric

  case class LowWatermarkReached(clientId: ClientId, group: Group, partitionsToResume: Set[TopicPartition]) extends EventLoopMetric

}

sealed trait EventLoopState

object EventLoopState {

  case object Running extends EventLoopState

  case object Paused extends EventLoopState

  case object ShuttingDown extends EventLoopState

}

