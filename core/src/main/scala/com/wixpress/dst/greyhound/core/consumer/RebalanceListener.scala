package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.TopicPartition
import zio.{UIO, URIO, ZIO}

trait RebalanceListener[-R] { self =>
  def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition]): URIO[R, DelayedRebalanceEffect]
  def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): URIO[R, Any]

  def *>[R1] (other: RebalanceListener[R1]) = new RebalanceListener[R with R1] {
    override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition]): URIO[R with R1, DelayedRebalanceEffect] =
      for {
        ef1 <-  self.onPartitionsRevoked(consumer, partitions)
        ef2 <- other.onPartitionsRevoked(consumer, partitions)
      } yield ef1 *> ef2

    override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): URIO[R with R1, Any] =
      self.onPartitionsAssigned(consumer, partitions) *> other.onPartitionsAssigned(consumer, partitions)
  }

  def provide(r: R) = new RebalanceListener[Any] {
    override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition]): URIO[Any, DelayedRebalanceEffect] = self.onPartitionsRevoked(consumer, partitions).provide(r)
    override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): URIO[Any, Any] = self.onPartitionsAssigned(consumer, partitions).provide(r)
  }
}

/**
  * An effect that needs to be called in the same thread as `KafkaConsumer.poll`.
  */
sealed trait DelayedRebalanceEffect { self =>
  private [greyhound] def run(): Unit
  def *>(that: DelayedRebalanceEffect) = new DelayedRebalanceEffect {
    override private[greyhound] def run(): Unit = {
      self.run()
      that.run()
    }
  }
  def lift = UIO(this)
  def toZIO = ZIO.effect(run())
  def tapError(f: Throwable => Unit) = new DelayedRebalanceEffect {
    override private[greyhound] def run(): Unit = try {
      self.run()
    } catch {
      case e: Throwable  =>
        f(e)
        throw e
    }
  }
  def catchAll(f: Throwable => Unit) = new DelayedRebalanceEffect {
    override private[greyhound] def run(): Unit = try {
      self.run()
    } catch {
      case e: Throwable  => f(e)
    }
  }
}

object DelayedRebalanceEffect {
  def unit = DelayedRebalanceEffect(())
  def zioUnit = unit.lift
  private [greyhound] def apply(effect : => Unit): DelayedRebalanceEffect = new DelayedRebalanceEffect{
    override private[greyhound] def run(): Unit = effect
  }
}

object RebalanceListener {
  val Empty = RebalanceListener()
  def make(onAssigned: (Consumer, Set[TopicPartition]) => UIO[Any] = (_,_) => ZIO.unit, onRevoked: (Consumer, Set[TopicPartition]) => UIO[Any] = (_,_) => ZIO.unit): RebalanceListener[Any] =
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition]): UIO[DelayedRebalanceEffect] = onRevoked(consumer, partitions).as(DelayedRebalanceEffect.unit)
      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): UIO[Any] = onAssigned(consumer, partitions)
    }

  def apply(onAssigned: Set[TopicPartition] => UIO[Any] = _ => ZIO.unit, onRevoked: Set[TopicPartition] => UIO[Any] = _ => ZIO.unit): RebalanceListener[Any] =
    new RebalanceListener[Any] {
      override def onPartitionsRevoked(consumer: Consumer, partitions: Set[TopicPartition]): UIO[DelayedRebalanceEffect] = onRevoked(partitions).as(DelayedRebalanceEffect.unit)
      override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): UIO[Any] = onAssigned(partitions)
    }
}
