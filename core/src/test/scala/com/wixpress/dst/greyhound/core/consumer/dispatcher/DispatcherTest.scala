package com.wixpress.dst.greyhound.core.consumer.dispatcher

import com.wixpress.dst.greyhound.core.Headers
import com.wixpress.dst.greyhound.core.consumer.Dispatcher.Record
import com.wixpress.dst.greyhound.core.consumer.DispatcherMetric.RecordHandled
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.consumer.{Dispatcher, SubmitResult}
import com.wixpress.dst.greyhound.core.consumer.SubmitResult.Rejected
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, TopicPartition}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.testkit._
import org.specs2.specification.Scope
import zio.clock.Clock
import zio.duration._
import zio.test.environment.TestClock
import zio.{test, _}

class DispatcherTest extends BaseTest[Env with TestClock with TestMetrics] {

  override def env: UManaged[Env with TestClock with TestMetrics] =
    for {
      env <- test.environment.testEnvironment.build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  "handle submitted records" in new ctx() {
    run(for {
      promise <- Promise.make[Nothing, Record]
      dispatcher <- Dispatcher.make("group", "clientId", promise.succeed, lowWatermark, highWatermark)
      _ <- submit(dispatcher, record)
      handled <- promise.await
    } yield handled must equalTo(record))
  }

  "parallelize handling based on partition" in new ctx() {
    val partitions = 8

    run(for {
      latch <- CountDownLatch.make(partitions)
      slowHandler = { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        clock.sleep(1.second) *> latch.countDown
      }
      dispatcher <- Dispatcher.make("group", "clientId", slowHandler, lowWatermark, highWatermark)
      _ <- ZIO.foreach_(0 until partitions) { partition =>
        submit(dispatcher, record.copy(partition = partition))
      }
      _ <- TestClock.adjust(1.second)
      _ <- latch.await
    } yield ok) // If execution is not parallel, the latch will not be released
  }

  "reject records when high watermark is reached" in new ctx() {
    run(for {
      dispatcher <- Dispatcher.make[Clock]("group", "clientId", _ => ZIO.never, lowWatermark, highWatermark)
      _ <-  submit(dispatcher, record.copy(offset = 0L)) // Will be polled
      _ <-  submit(dispatcher, record.copy(offset = 1L))
      _ <-  submit(dispatcher, record.copy(offset = 2L))
      _ <-  submit(dispatcher, record.copy(offset = 3L))
      _ <-  submit(dispatcher, record.copy(offset = 4L))
      result1 <-  submit(dispatcher, record.copy(offset = 5L)) // maybe will be dropped
      result2 <-  submit(dispatcher, record.copy(offset = 6L)) // maybe will be dropped
    } yield (result1 must equalTo(SubmitResult.Rejected)) or (result2 must equalTo(SubmitResult.Rejected)))
  }

  "resume paused partitions" in new ctx(lowWatermark = 3, highWatermark = 7) {
    run(
      for {
        queue <- Queue.bounded[Record](1)
        dispatcher <- Dispatcher.make[Clock]("group", "clientId", (record) =>  queue.offer(record).flatMap(result => UIO(println(s"queue.offer result: ${result}"))), lowWatermark, highWatermark)
        _ <- ZIO.foreach_(0 to (highWatermark + 1)) { offset =>
           submit(dispatcher, ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L))
        }
        _ <-  submit(dispatcher, ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 6L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)) // Will be dropped
        _ <- eventuallyZ(dispatcher.resumeablePartitions(Set(topicPartition)))(_.isEmpty)
        _ <- ZIO.foreach_(1 to 4 )(_ => queue.take)
        _ <- eventuallyZ(dispatcher.resumeablePartitions(Set(topicPartition)))(_ == Set(TopicPartition(topic, partition)))
      } yield ok
    )
  }

  "block resume paused partitions" in new ctx(lowWatermark = 3, highWatermark = 7) {
    run(
      for {
        queue <- Queue.bounded[Record](1)
        dispatcher <- Dispatcher.make[TestClock]("group", "clientId", (record) =>  queue.offer(record).flatMap(result => UIO(println(s"block resume paused partitions -queue.offer result: ${result}"))),
          lowWatermark, highWatermark, 6500)
        _ <- ZIO.foreach_(0 to (highWatermark + 1)) { offset =>
          submit(dispatcher, ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L))
        }
        overCapacitySubmitResult <-  submit(dispatcher, ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 6L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)) // Will be dropped
        resumeablePartitionsWhenInHighWatermark <- dispatcher.resumeablePartitions(Set(topicPartition))
        _ <- ZIO.foreach_(1 to 4 )(_ => queue.take)
        _ <- TestClock.adjust(1.second)
        resumablePartitionDuringBlockPeriod <- dispatcher.resumeablePartitions(Set(topicPartition))
        _ <- TestClock.adjust(6.second)
        resumablePartitionAfterBlockPeriod <- dispatcher.resumeablePartitions(Set(topicPartition))
        _ <- ZIO.foreach_(0 to 3) { offset =>
          submit(dispatcher, ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L))
        }
        overCapacitySubmitResult2 <-  submit(dispatcher, ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 16L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)) // Will be dropped
        _ <- ZIO.foreach_(1 to 4 )(_ => queue.take)
        _ <- TestClock.adjust(1.second)
        // test clearPausedPartitionDuration
        resumablePartitionDuringBlockPeriod2 <- dispatcher.resumeablePartitions(Set(topicPartition))
      } yield (resumeablePartitionsWhenInHighWatermark aka "resumeablePartitionsWhenInHighWatermark" must beEmpty) and
       (resumablePartitionDuringBlockPeriod aka "resumablePartitionDuringBlockPeriod" must beEmpty) and
       (resumablePartitionAfterBlockPeriod aka "resumablePartitionAfterBlockPeriod" mustEqual Set(TopicPartition(topic, partition))) and
        (overCapacitySubmitResult aka "overCapacitySubmitResult" mustEqual Rejected) and
        (overCapacitySubmitResult2 aka "overCapacitySubmitResult2" mustEqual Rejected) and
        (resumablePartitionDuringBlockPeriod2 aka "resumablePartitionDuringBlockPeriod2" must beEmpty)
    )


    private def submit(dispatcher: Dispatcher[TestClock], record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]): URIO[TestClock with Env, SubmitResult] = {
      dispatcher.submit(record).tap(_ => TestClock.adjust(10.millis))
    }
  }

  "pause handling" in new ctx() {
    run(for {
      ref <- Ref.make(0)
      dispatcher <- Dispatcher.make[Clock]("group", "clientId", _ => ref.update(_ + 1), lowWatermark, highWatermark)
      _ <- pause(dispatcher)
      _ <- submit(dispatcher, record) // Will be queued
      invocations <- ref.get
    } yield invocations must equalTo(0))
  }

  "complete executing task before pausing" in new ctx() {
    run(for {
      ref <- Ref.make(0)
      promise <- Promise.make[Nothing, Unit]
      handler = { _: Record =>
        clock.sleep(1.second) *>
          ref.update(_ + 1) *>
          promise.succeed(())
      }
      dispatcher <- Dispatcher.make[Clock]("group", "clientId", handler, lowWatermark, highWatermark)
      _ <- submit(dispatcher, record) // Will be handled
      _ <- TestMetrics.reported.flatMap(waitUntilRecordHandled(3.seconds))
      _ <- pause(dispatcher)
      _ <- submit(dispatcher, record) // Will be queued
      _ <- TestClock.adjust(1.second)
      _ <- promise.await
      invocations <- ref.get
    } yield invocations must equalTo(1))
  }

  private def submit(dispatcher: Dispatcher[Clock], record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]): URIO[TestClock with Env, SubmitResult] = {
    dispatcher.submit(record).tap(_ => TestClock.adjust(10.millis))
  }

  private def waitUntilRecordHandled(timeout: Duration)(metrics: Seq[GreyhoundMetric]) =
    ZIO.when(metrics.collect { case r: RecordHandled[_, _] => r }.nonEmpty)(ZIO.fail(TimeoutWaitingForHandledMetric))
      .retry(Schedule.duration(timeout))

  "resume handling" in new ctx() {
    run(for {
      ref <- Ref.make(0)
      promise <- Promise.make[Nothing, Unit]
      handler = { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        ref.update(_ + 1) *> promise.succeed(())
      }
      dispatcher <- Dispatcher.make("group", "clientId", handler, lowWatermark, highWatermark)
      _ <- pause(dispatcher)
      _ <- submit(dispatcher, record)
      _ <- resume(dispatcher)
      _ <- promise.await
      invocations <- ref.get
    } yield invocations must equalTo(1))
  }

  private def resume(dispatcher: Dispatcher[Nothing]) = {
    dispatcher.resume *> TestClock.adjust(10.millis)
  }

  private def pause(dispatcher: Dispatcher[Nothing]) = {
    dispatcher.pause *> TestClock.adjust(10.millis)
  }

  abstract class ctx(val lowWatermark: Int = 2, val highWatermark: Int = 4) extends Scope {
    val topic = "topic"
    val partition = 0
    val topicPartition = TopicPartition(topic, partition)
    val record = ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 0L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
  }

}

case object TimeoutWaitingForHandledMetric extends GreyhoundMetric
