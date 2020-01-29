package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Headers
import com.wixpress.dst.greyhound.core.consumer.Dispatcher.Record
import com.wixpress.dst.greyhound.core.consumer.DispatcherTest._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, CountDownLatch, TestMetrics}
import zio._
import zio.duration._
import zio.test.environment.{TestClock, TestEnvironment}

class DispatcherTest extends BaseTest[TestClock with TestMetrics] {

  override def env: UManaged[TestClock with TestMetrics] =
    for {
      env <- TestEnvironment.Value
      testMetrics <- TestMetrics.make
    } yield new TestClock with TestMetrics {
      override val clock: TestClock.Service[Any] = env.clock
      override val scheduler: TestClock.Service[Any] = env.scheduler
      override val metrics: TestMetrics.Service = testMetrics.metrics
    }

  "handle submitted records" in {
    for {
      promise <- Promise.make[Nothing, Record]
      makeDispatcher = Dispatcher.make(promise.succeed(_).unit, lowWatermark, highWatermark)
      handled <- makeDispatcher.use { dispatcher =>
        dispatcher.submit(record) *>
          promise.await
      }
    } yield handled must equalTo(record)
  }

  "notify when handling is done" in {
    for {
      ref <- Ref.make(Option.empty[Record])
      makeDispatcher = Dispatcher.make(
        record => ref.set(Some(record)).delay(1.second),
        lowWatermark,
        highWatermark)
      fiber <- makeDispatcher.use { dispatcher =>
        dispatcher.submit(record).flatMap {
          case SubmitResult.Submitted(done) => done
          case SubmitResult.Rejected => ZIO.unit
        } *> ref.get
      }.fork
      _ <- TestClock.adjust(1.second)
      handled <- fiber.join
    } yield handled must beSome(record)
  }

  "parallelize handling based on partition" in {
    val partitions = 8

    for {
      latch <- CountDownLatch.make(partitions)
      slowHandler = { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        clock.sleep(1.second) *> latch.countDown
      }
      _ <- Dispatcher.make(slowHandler, lowWatermark, highWatermark).use { dispatcher =>
        ZIO.foreach(0 until partitions) { partition =>
          dispatcher.submit(record.copy(partition = partition))
        } *> TestClock.adjust(1.second) *> latch.await
      }
    } yield ok // If execution is not parallel, the latch will not be released
  }

  "reject records when high watermark is reached" in {
    for {
      result <- Dispatcher.make(_ => ZIO.never, lowWatermark, highWatermark).use { dispatcher =>
        dispatcher.submit(record.copy(offset = 0L)) *> // Will be polled
          dispatcher.submit(record.copy(offset = 1L)) *>
          dispatcher.submit(record.copy(offset = 2L)) *>
          dispatcher.submit(record.copy(offset = 3L)) *>
          dispatcher.submit(record.copy(offset = 4L)) *>
          dispatcher.submit(record.copy(offset = 5L)) // Will be dropped
      }
    } yield result must equalTo(SubmitResult.Rejected)
  }

  "resume paused partitions" in {
    Queue.bounded[Record](1).flatMap { queue =>
      Dispatcher.make(queue.offer(_).unit, lowWatermark, highWatermark).use { dispatcher =>
        for {
          _ <- ZIO.foreach(0 to (highWatermark + 1)) { offset =>
            dispatcher.submit(ConsumerRecord(topic, partition, offset, Headers.Empty, None, Chunk.empty))
          }
          _ <- dispatcher.submit(ConsumerRecord(topic, partition, 6L, Headers.Empty, None, Chunk.empty)) // Will be dropped
          partitionsToResume1 <- dispatcher.resumeablePartitions(Set(topicPartition))
          _ <- queue.take *> queue.take
          partitionsToResume2 <- dispatcher.resumeablePartitions(Set(topicPartition))
        } yield (partitionsToResume1 must beEmpty) and
          (partitionsToResume2 must equalTo(Set(TopicPartition(topic, partition))))
      }
    }
  }

}

object DispatcherTest {
  val lowWatermark = 2
  val highWatermark = 4

  val topic = "topic"
  val partition = 0
  val topicPartition = TopicPartition(topic, partition)
  val record = ConsumerRecord(topic, partition, 0L, Headers.Empty, None, Chunk.empty)
}
