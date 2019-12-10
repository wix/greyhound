package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.EventLoopCommand.{Pause, Resume}
import com.wixpress.dst.greyhound.core.consumer.EventLoopState.{Paused, Polling}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{CommittingOffsets, Metrics, Subscribing}
import com.wixpress.dst.greyhound.core.{Record, TopicName}
import zio.blocking.Blocking
import zio.duration._
import zio._

import scala.collection.JavaConverters._

trait EventLoop {
  def pause: UIO[Unit]
  def resume: UIO[Unit]
}

object EventLoop {
  private val pollTimeout = 100.millis

  def make(consumer: Consumer,
           offsets: ParallelRecordHandler.OffsetsMap,
           handler: ParallelRecordHandler.Handler,
           topics: Set[TopicName]): ZManaged[Blocking with GreyhoundMetrics, Throwable, EventLoop] = {
    val eventLoop = for {
      _ <- subscribe(consumer, topics)
      state <- Ref.make[EventLoopState](Polling)
      commands <- Queue.bounded[(Promise[Nothing, Unit], EventLoopCommand)](1)
      resumes <- Queue.bounded[Unit](1)

      _ <- state.get.flatMap {
        case Polling => pollAndHandle(consumer, handler) *> commitOffsets(consumer, offsets)
        case Paused(promise) => promise.succeed(()) *> resumes.take
      }.forever.fork

      _ <- commands.take.flatMap {
        case (promise, Resume) => state.set(Polling) *> resumes.offer(()) *> promise.succeed(())
        case (promise, Pause) => state.set(Paused(promise))
      }.forever.fork

    } yield new EventLoop {
      override val pause: UIO[Unit] = sendCommand(Pause)

      override val resume: UIO[Unit] = sendCommand(Resume)

      private def sendCommand(command: EventLoopCommand): UIO[Unit] = for {
        promise <- Promise.make[Nothing, Unit]
        _ <- commands.offer((promise, command))
        _ <- promise.await
      } yield ()
    }

    // TODO add timeout to `pause` on shutdown?
    // TODO add visibility
    eventLoop.toManaged(_.pause)
  }

  private def subscribe(consumer: Consumer, topics: Set[TopicName]) =
    Metrics.report(Subscribing(topics)) *> consumer.subscribe(topics)

  private def pollAndHandle(consumer: Consumer, handler: ParallelRecordHandler.Handler) =
    consumer.poll(pollTimeout).flatMap { records =>
      ZIO.foreach_(records.asScala) { record =>
        handler.handle(Record(record))
      }
    }

  private def commitOffsets(consumer: Consumer, offsets: ParallelRecordHandler.OffsetsMap) =
    offsets.modify(current => (current, Map.empty)).flatMap { current =>
      ZIO.when(current.nonEmpty) {
        Metrics.report(CommittingOffsets(current)) *>
          consumer.commit(current)
      }
    }
}

sealed trait EventLoopState

object EventLoopState {
  case object Polling extends EventLoopState
  case class Paused(promise: Promise[Nothing, Unit]) extends EventLoopState
}

sealed trait EventLoopCommand

object EventLoopCommand {
  case object Pause extends EventLoopCommand
  case object Resume extends EventLoopCommand
}
