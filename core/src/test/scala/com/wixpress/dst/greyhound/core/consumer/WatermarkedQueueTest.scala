package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Headers
import com.wixpress.dst.greyhound.core.consumer.WatermarkedQueueTest._
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import zio.{Managed, UManaged}

class WatermarkedQueueTest extends BaseTest[Any] {

  override def env: UManaged[Any] = Managed.unit

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
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "bar"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "baz"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "qux"))
      partitionsToPause <- queue.pausePartitions
    } yield partitionsToPause must equalTo(Set(TopicPartition(topic, 3)))
  }

  "clear partitions to pause" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "bar"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "baz"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "qux"))
      _ <- queue.pausePartitions
      partitionsToPause <- queue.pausePartitions
    } yield partitionsToPause must beEmpty
  }

  "add paused partitions to existing" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "foo"))
      partitionsToPause1 <- queue.pausePartitions
      _ <- queue.offer(ConsumerRecord(topic, 4, 0L, Headers.Empty, None, "foo"))
      partitionsToPause2 <- queue.pausePartitions
      _ <- queue.take
      _ <- queue.take
      _ <- queue.take
      partitionsToResume <- queue.resumePartitions
    } yield partitionsToResume must equalTo(partitionsToPause1 union partitionsToPause2)
  }

  "resume paused partitions" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "bar"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "baz"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "qux"))
      _ <- queue.pausePartitions
      partitionsToResume1 <- queue.resumePartitions
      _ <- queue.take
      _ <- queue.take
      partitionsToResume2 <- queue.resumePartitions
    } yield (partitionsToResume1 must beEmpty) and
      (partitionsToResume2 must equalTo(Set(TopicPartition(topic, 3))))
  }

  "clear resumed partitions" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "bar"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "baz"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "qux"))
      _ <- queue.pausePartitions
      _ <- queue.take
      _ <- queue.take
      _ <- queue.resumePartitions
      partitionsToResume <- queue.resumePartitions
    } yield partitionsToResume must beEmpty
  }

  "clear partitions to pause if a enough records were polled before `pausePartitions` was called" in {
    for {
      queue <- makeQueue
      _ <- queue.offer(ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo"))
      _ <- queue.offer(ConsumerRecord(topic, 1, 0L, Headers.Empty, None, "bar"))
      _ <- queue.offer(ConsumerRecord(topic, 2, 0L, Headers.Empty, None, "baz"))
      _ <- queue.offer(ConsumerRecord(topic, 3, 0L, Headers.Empty, None, "qux"))
      _ <- queue.take
      _ <- queue.take
      partitionsToPause <- queue.pausePartitions
    } yield partitionsToPause must beEmpty
  }

}

object WatermarkedQueueTest {
  val lowWatermark = 2
  val highWatermark = 4
  val capacity = 8

  val topic = "topic"
  val record = ConsumerRecord(topic, 0, 0L, Headers.Empty, None, "foo")
  val config = WatermarkedQueueConfig(lowWatermark, highWatermark, capacity)
  val makeQueue = WatermarkedQueue.make[Nothing, String](config)
}
