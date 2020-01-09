package com.wixpress.dst.greyhound.core.consumer

import zio.{Queue, Ref, UIO}

trait WatermarkedQueue[K, V] {
  def offer(record: ConsumerRecord[K, V]): UIO[Unit]

  def take: UIO[ConsumerRecord[K, V]]

  def shutdown: UIO[Unit]

  def pausePartitions: UIO[Set[TopicPartition]]

  def resumePartitions: UIO[Set[TopicPartition]]
}

object WatermarkedQueue {
  // TODO should we drop offered records and seek to first offset over high watermark?
  def make[K, V](config: WatermarkedQueueConfig): UIO[WatermarkedQueue[K, V]] = for {
    state <- Ref.make(State(config.lowWatermark, config.highWatermark))
    queue <-  Queue.bounded[ConsumerRecord[K, V]](config.capacity)
  } yield new WatermarkedQueue[K, V] {
    override def offer(record: ConsumerRecord[K, V]): UIO[Unit] =
      state.update(_.offer(record)) *> queue.offer(record).unit

    override def take: UIO[ConsumerRecord[K, V]] =
      state.update(_.take) *> queue.take

    override def shutdown: UIO[Unit] =
      queue.shutdown

    override def pausePartitions: UIO[Set[TopicPartition]] =
      state.modify(_.pausePartitions)

    override def resumePartitions: UIO[Set[TopicPartition]] =
      state.modify(_.resumePartitions)
  }

  case class State(lowWatermark: Int,
                   highWatermark: Int,
                   size: Int = 0,
                   pausedPartitions: Set[TopicPartition] = Set.empty,
                   partitionsToPause: Set[TopicPartition] = Set.empty) {

    def offer(record: ConsumerRecord[_, _]): State = {
      val newSize = size + 1
      val newPartitionsToPause =
        if (newSize >= highWatermark) partitionsToPause + TopicPartition(record)
        else partitionsToPause

      copy(size = newSize, partitionsToPause = newPartitionsToPause)
    }

    def take: State = {
      val newSize = size - 1
      val newPartitionsToPause =
        if (newSize <= lowWatermark) Set.empty[TopicPartition]
        else partitionsToPause

      copy(size = newSize, partitionsToPause = newPartitionsToPause)
    }

    def pausePartitions: (Set[TopicPartition], State) = {
      val newPausedPartitions = pausedPartitions union partitionsToPause
      (partitionsToPause, copy(pausedPartitions = newPausedPartitions, partitionsToPause = Set.empty))
    }

    def resumePartitions: (Set[TopicPartition], State) =
      if (size <= lowWatermark) (pausedPartitions, copy(pausedPartitions = Set.empty))
      else (Set.empty, this)

  }

}

case class WatermarkedQueueConfig(lowWatermark: Int, highWatermark: Int, capacity: Int)

object WatermarkedQueueConfig {
  val Default = WatermarkedQueueConfig(lowWatermark = 100, highWatermark = 200, capacity = 256)
}
