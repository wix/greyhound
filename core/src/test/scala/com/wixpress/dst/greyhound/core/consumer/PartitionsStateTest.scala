package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Offset
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import zio.{Ref, UIO, UManaged, ZIO, ZManaged}

class PartitionsStateTest extends BaseTest[Any] {

  override def env: UManaged[Any] = ZManaged.unit

  "combine" should {
    "pause both" in {
      for {
        ref <- Ref.make(0)
        partitionsState = new EmptyPartitionsState {
          override def pause: UIO[Unit] =
            ref.update(_ + 1).unit
        }
        combined = partitionsState combine partitionsState
        _ <- combined.pause
        paused <- ref.get
      } yield paused must equalTo(2)
    }

    "resume both" in {
      for {
        ref <- Ref.make(0)
        partitionsState = new EmptyPartitionsState {
          override def resume: UIO[Unit] =
            ref.update(_ + 1).unit
        }
        combined = partitionsState combine partitionsState
        _ <- combined.resume
        resumed <- ref.get
      } yield resumed must equalTo(2)
    }

    "return all partitions to resume" in {
      val partition1 = TopicPartition("foo", 0)
      val partition2 = TopicPartition("bar", 0)
      val partitionsState1 = new EmptyPartitionsState {
        override def partitionsToResume: UIO[Set[TopicPartition]] =
          ZIO.succeed(Set(partition1))
      }
      val partitionsState2 = new EmptyPartitionsState {
        override def partitionsToResume: UIO[Set[TopicPartition]] =
          ZIO.succeed(Set(partition2))
      }

      (partitionsState1 combine partitionsState2).partitionsToResume.map { partitions =>
        partitions must contain(partition1, partition2)
      }
    }

    "return all partitions to pause" in {
      val partition1 = TopicPartition("foo", 0)
      val partition2 = TopicPartition("bar", 0)
      val partitionsState1 = new EmptyPartitionsState {
        override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
          ZIO.succeed(Map(partition1 -> 0L))
      }
      val partitionsState2 = new EmptyPartitionsState {
        override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
          ZIO.succeed(Map(partition2 -> 1L))
      }

      (partitionsState1 combine partitionsState2).partitionsToPause.map { partitions =>
        partitions must havePairs(partition1 -> 0L, partition2 -> 1L)
      }
    }

    "merge existing keys by taking the minimum offset" in {
      val partition1 = TopicPartition("foo", 0)
      val partition2 = TopicPartition("bar", 0)
      val partitionsState1 = new EmptyPartitionsState {
        override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
          ZIO.succeed(Map(partition1 -> 0L, partition2 -> 0L))
      }
      val partitionsState2 = new EmptyPartitionsState {
        override def partitionsToPause: UIO[Map[TopicPartition, Offset]] =
          ZIO.succeed(Map(partition1 -> 1L))
      }

      (partitionsState1 combine partitionsState2).partitionsToPause.map { partitions =>
        partitions must havePairs(partition1 -> 0L, partition2 -> 0L)
      }
    }
  }

}
