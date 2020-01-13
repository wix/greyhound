package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Offset
import zio.{Queue, Ref, UIO, ZIO}

trait WatermarkedQueue[K, V] {
  def offer(record: ConsumerRecord[K, V]): UIO[Boolean]

  def take: UIO[ConsumerRecord[K, V]]

  def shutdown: UIO[Unit] // TODO is this needed?

  def pausePartitions: UIO[Map[TopicPartition, Offset]]

  def resumePartitions: UIO[Set[TopicPartition]]
}

/**
  * This implementation is not fiber-safe. Since the queue is used per partition-bucket,
  * and all operations performed on a single bucket are linearizable this is fine.
  */
object WatermarkedQueue {
  def make[K, V](config: WatermarkedQueueConfig): UIO[WatermarkedQueue[K, V]] = for {
    state <- Ref.make(State.Empty)
    queue <-  Queue.dropping[ConsumerRecord[K, V]](config.highWatermark)
  } yield new WatermarkedQueue[K, V] {
    override def offer(record: ConsumerRecord[K, V]): UIO[Boolean] =
      queue.offer(record).tap(added => state.update(_.offer(record, added)))

    override def take: UIO[ConsumerRecord[K, V]] =
      queue.take

    override def shutdown: UIO[Unit] =
      queue.shutdown

    override def pausePartitions: UIO[Map[TopicPartition, Offset]] =
      state.modify(_.pausePartitions)

    override def resumePartitions: UIO[Set[TopicPartition]] =
      queue.size.flatMap { size =>
        if (size <= config.lowWatermark) state.modify(_.resumePartitions)
        else ZIO.succeed(Set.empty)
      }
  }

  case class State(pausedPartitions: Set[TopicPartition],
                   partitionsToPause: Map[TopicPartition, Offset]) {

    def offer(record: ConsumerRecord[_, _], added: Boolean): State = {
      val partition = TopicPartition(record)
      val newPartitionsToPause =
        if (added || partitionsToPause.contains(partition)) partitionsToPause
        else partitionsToPause + (partition -> record.offset)

      copy(partitionsToPause = newPartitionsToPause)
    }

    def pausePartitions: (Map[TopicPartition, Offset], State) = {
      val newPausedPartitions = pausedPartitions union partitionsToPause.keySet
      (partitionsToPause, copy(pausedPartitions = newPausedPartitions, partitionsToPause = Map.empty))
    }

    def resumePartitions: (Set[TopicPartition], State) =
      (pausedPartitions, copy(pausedPartitions = Set.empty))

  }

  object State {
    val Empty = State(Set.empty, Map.empty)
  }

}

case class WatermarkedQueueConfig(lowWatermark: Int, highWatermark: Int)

object WatermarkedQueueConfig {
  val Default = WatermarkedQueueConfig(lowWatermark = 128, highWatermark = 256)
}
