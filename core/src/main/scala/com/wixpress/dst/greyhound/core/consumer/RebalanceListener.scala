package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.TopicPartition
import zio.{Tag, Trace, UIO, URIO, ZEnvironment, ZIO, ZLayer}

trait RebalanceListener[-R] { self =>
  def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(implicit trace: Trace): URIO[R, DelayedRebalanceEffect]
  def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(implicit trace: Trace): URIO[R, DelayedRebalanceEffect]

  def *>[R1](other: RebalanceListener[R1]) = new RebalanceListener[R with R1] {
    override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
      implicit trace: Trace
    ): URIO[R with R1, DelayedRebalanceEffect] =
      for {
        ef1 <- self.onPartitionsRevoked(consumer, partitions)
        ef2 <- other.onPartitionsRevoked(consumer, partitions)
      } yield ef1 *> ef2

    override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
      implicit trace: Trace
    ): URIO[R with R1, DelayedRebalanceEffect] =
      self.onPartitionsAssigned(consumer, partitions) *> other.onPartitionsAssigned(consumer, partitions)
  }

  def provide[R1 <: R: Tag](r: R1)(implicit trace: Trace) = new RebalanceListener[Any] {
    override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
      implicit trace: Trace
    ): URIO[Any, DelayedRebalanceEffect] =
      self.onPartitionsRevoked(consumer, partitions).provide(ZLayer.succeed(r))
    override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
      implicit trace: Trace
    ): URIO[Any, DelayedRebalanceEffect] =
      self.onPartitionsAssigned(consumer, partitions).provide(ZLayer.succeed(r))
  }

  def provideEnvironment[R1 <: R](r: ZEnvironment[R1])(implicit trace: Trace) = new RebalanceListener[Any] {
    override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
      implicit trace: Trace
    ): URIO[Any, DelayedRebalanceEffect] =
      self.onPartitionsRevoked(consumer, partitions).provideEnvironment(r)
    override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
      implicit trace: Trace
    ): URIO[Any, DelayedRebalanceEffect] =
      self.onPartitionsAssigned(consumer, partitions).provideEnvironment(r)
  }
}

/**
 * An effect that needs to be called in the same thread as `KafkaConsumer.poll`.
 */
sealed trait DelayedRebalanceEffect { self =>
  private[greyhound] def run(): Unit
  def *>(that: DelayedRebalanceEffect) = new DelayedRebalanceEffect {
    override private[greyhound] def run(): Unit = {
      self.run()
      that.run()
    }
  }
  def lift(implicit trace: Trace)      = ZIO.succeed(this)
  def toZIO(implicit trace: Trace)     = ZIO.attempt(run())
  def tapError(f: Throwable => Unit)   = new DelayedRebalanceEffect {
    override private[greyhound] def run(): Unit = try {
      self.run()
    } catch {
      case e: Throwable =>
        f(e)
        throw e
    }
  }
  def catchAll(f: Throwable => Unit)   = new DelayedRebalanceEffect {
    override private[greyhound] def run(): Unit = try {
      self.run()
    } catch {
      case e: Throwable => f(e)
    }
  }
}

object DelayedRebalanceEffect {
  def unit                                                              = DelayedRebalanceEffect(())
  def zioUnit(implicit trace: Trace)                                    = unit.lift
  private[greyhound] def apply(effect: => Unit): DelayedRebalanceEffect = new DelayedRebalanceEffect {
    override private[greyhound] def run(): Unit = effect
  }
}

object RebalanceListener {
  val Empty = RebalanceListener()
  def make[R](
    onAssigned: (Consumer, Set[TopicPartition]) => URIO[R, Any] = (c: Consumer, tps: Set[TopicPartition]) => ZIO.unit,
    onRevoked: (Consumer, Set[TopicPartition]) => URIO[R, Any] = (c: Consumer, tps: Set[TopicPartition]) => ZIO.unit
  ): RebalanceListener[R] =
    new RebalanceListener[R] {
      override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): URIO[R, DelayedRebalanceEffect] =
        onRevoked(consumer, partitions).as(DelayedRebalanceEffect.unit)
      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): URIO[R, DelayedRebalanceEffect] =
        onAssigned(consumer, partitions).as(DelayedRebalanceEffect.unit)
    }

  def apply(
    onAssigned: Set[TopicPartition] => UIO[Any] = _ => ZIO.unit,
    onRevoked: Set[TopicPartition] => UIO[Any] = _ => ZIO.unit
  ): RebalanceListener[Any] =
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): UIO[DelayedRebalanceEffect] =
        onRevoked(partitions).as(DelayedRebalanceEffect.unit)
      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
        implicit trace: Trace
      ): UIO[DelayedRebalanceEffect] =
        onAssigned(partitions).as(DelayedRebalanceEffect.unit)
    }
}
