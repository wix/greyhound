package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Headers
import com.wixpress.dst.greyhound.core.consumer.WatermarkedQueueTest._
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import zio.clock.Clock
import zio.duration._
import zio.{Managed, UManaged, ZIO}

class WatermarkedQueueTest extends BaseTest[Clock] {

  override def env: UManaged[Clock] =
    Managed.succeed(Clock.Live)

  "offer and poll from queue" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(record)
      polled <- queue.take
    } yield polled must equalTo(record)
  }

  "pause partitions when high watermark is reached" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "record-1"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 1L, Headers.Empty, None, "record-2"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 2L, Headers.Empty, None, "record-3"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 3L, Headers.Empty, None, "record-4"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 4L, Headers.Empty, None, "record-5")) // Will be dropped
      partitionsToPause <- queue.partitionsToPause
    } yield partitionsToPause must equalTo(Map(TopicPartition(topic, 0) -> 4L))
  }

  "keep first offset per partition to pause" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "record-1"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 1L, Headers.Empty, None, "record-2"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 2L, Headers.Empty, None, "record-3"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 3L, Headers.Empty, None, "record-4"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 4L, Headers.Empty, None, "record-5")) // Will be dropped
      _ <- queue.offer(ConsumerRecord(topic, 0, 5L, Headers.Empty, None, "record-6")) // Will be dropped
      _ <- queue.offer(ConsumerRecord(topic, 1, 6L, Headers.Empty, None, "record-7")) // Will be dropped
      partitionsToPause <- queue.partitionsToPause
    } yield partitionsToPause must havePairs(TopicPartition(topic, 0) -> 4L, TopicPartition(topic, 1) -> 6L)
  }

  "drop new records after queue is full" in {
    val firstRecords = List(
      ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "record-1"),
      ConsumerRecord(topic, 0, 1L, Headers.Empty, None, "record-2"),
      ConsumerRecord(topic, 0, 2L, Headers.Empty, None, "record-3"),
      ConsumerRecord(topic, 0, 3L, Headers.Empty, None, "record-4"))

    for {
      queue <- makeQueue
      addedFirst <- ZIO.foreach(firstRecords)(queue.offer)
      addedNext <- queue.offer(ConsumerRecord(topic, 0, 4L, Headers.Empty, None, "record-5")) // Will be dropped
      records <- ZIO.collectAll(List.fill(4)(queue.take))
      next <- queue.take.timeout(1.milli)
    } yield
      (addedFirst must forall(beTrue)) and
        (records must equalTo(firstRecords)) and
        (addedNext must beFalse) and
        (next must beNone)
  }

  "clear partitions to pause" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "record-1"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 1L, Headers.Empty, None, "record-2"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 2L, Headers.Empty, None, "record-3"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 3L, Headers.Empty, None, "record-4"))
      _ <- queue.offer(ConsumerRecord(topic, 0, 4L, Headers.Empty, None, "record-5")) // Will be dropped
      _ <- queue.partitionsToPause
      partitionsToPause <- queue.partitionsToPause
    } yield partitionsToPause must beEmpty
  }

  "add paused partitions to existing" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "record-1"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "record-2"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "record-3"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "record-4"))
      _ <- queue.offer(ConsumerRecord(topic, 4, 0L, Headers.Empty, None, "record-5")) // Will be dropped
      _ <- queue.offer(ConsumerRecord(topic, 5, 0L, Headers.Empty, None, "record-6")) // Will be dropped
      _ <- queue.partitionsToPause
      _ <- queue.take
      _ <- queue.take
      partitionsToResume <- queue.partitionsToResume
    } yield partitionsToResume must equalTo(Set(TopicPartition(topic, 4), TopicPartition(topic, 5)))
  }

  "resume paused partitions" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "record-1"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "record-2"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "record-3"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "record-4"))
      _ <- queue.offer(ConsumerRecord(topic, 4, 0L, Headers.Empty, None, "record-5")) // Will be dropped
      _ <- queue.partitionsToPause
      partitionsToResume1 <- queue.partitionsToResume
      _ <- queue.take
      _ <- queue.take
      partitionsToResume2 <- queue.partitionsToResume
    } yield (partitionsToResume1 must beEmpty) and
      (partitionsToResume2 must equalTo(Set(TopicPartition(topic, 4))))
  }

  "clear resumed partitions" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "record-1"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "record-2"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "record-3"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "record-4"))
      _ <- queue.offer(ConsumerRecord(topic, 4, 0L, Headers.Empty, None, "record-5")) // Will be dropped
      _ <- queue.partitionsToPause
      _ <- queue.take
      _ <- queue.take
      _ <- queue.partitionsToResume
      partitionsToResume <- queue.partitionsToResume
    } yield partitionsToResume must beEmpty
  }

  "pause handling regardless of queue's size" in {
    for {
      queue <- makeQueue
      _ <- queue.pause
      _ <- queue.offer(record)
      partitionsToPause <- queue.partitionsToPause
    } yield partitionsToPause must havePair(TopicPartition(record) -> record.offset)
  }

  "drop new records if queue is paused" in {
    val record1 = ConsumerRecord(topic, 0, 1L, Headers.Empty, None, "record-1")
    val record2 = ConsumerRecord(topic, 0, 2L, Headers.Empty, None, "record-2")

    for {
      queue <- makeQueue
      _ <- queue.pause
      _ <- queue.offer(record1) // Will be dropped
      _ <- queue.resume
      _ <- queue.offer(record2)
      polled <- queue.take
    } yield polled must equalTo(record2)
  }

  "not resume any partitions when in paused state" in {
    for {
      queue <- makeQueue
      _ <- queue.pause
      _ <- queue.offer(ConsumerRecord(topic, 0, 1L, Headers.Empty, None, "record-1")) // Will be dropped
      _ <- queue.partitionsToPause
      partitionsToResume <- queue.partitionsToResume
    } yield partitionsToResume must beEmpty
  }

}

object WatermarkedQueueTest {
  val lowWatermark = 2
  val highWatermark = 4

  val topic = "topic"
  val record = ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo")
  val config = WatermarkedQueueConfig(lowWatermark, highWatermark)
  val makeQueue = WatermarkedQueue.make[Nothing, String](config)
}
