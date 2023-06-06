package com.wixpress.dst.greyhound.core.consumer.dispatcher

import com.wixpress.dst.greyhound.core.consumer.Dispatcher.Record
import com.wixpress.dst.greyhound.core.consumer.DispatcherMetric.RecordHandled
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.consumer.SubmitResult.Rejected
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.consumer.{Dispatcher, SubmitResult}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.testkit._
import com.wixpress.dst.greyhound.core.zioutils.AwaitShutdown.ShutdownPromise
import com.wixpress.dst.greyhound.core.zioutils.CountDownLatch
import com.wixpress.dst.greyhound.core.{Headers, TopicPartition}
import org.specs2.specification.Scope
import zio.test.TestClock
import zio.{durationInt, Chunk, Clock, DurationSyntax => _, Promise, Queue, Ref, URIO, ZIO}

class DispatcherTest extends BaseTest[TestMetrics with TestClock] {
  sequential

  override def env =
    for {
      clock       <- testClock
      testMetrics <- TestMetrics.makeManagedEnv
    } yield testMetrics ++ clock

  "handle submitted records" in
    new ctx() {
      run(for {
        promise    <- Promise.make[Nothing, Record]
        ref        <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        init       <- getInit
        dispatcher <-
          Dispatcher.make("group", "clientId", promise.succeed, lowWatermark, highWatermark, workersShutdownRef = ref, init = init)
        _          <- submit(dispatcher, record)
        handled    <- promise.await
      } yield handled must equalTo(record))
    }

  "parallelize handling based on partition" in
    new ctx() {
      val partitions = 8

      run(for {
        latch      <- CountDownLatch.make(partitions)
        slowHandler = { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => Clock.sleep(1.second) *> latch.countDown }
        ref        <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        init       <- getInit
        dispatcher <-
          Dispatcher.make("group", "clientId", slowHandler, lowWatermark, highWatermark, workersShutdownRef = ref, init = init)
        _          <- ZIO.foreachDiscard(0 until partitions) { partition => submit(dispatcher, record.copy(partition = partition)) }
        _          <- TestClock.adjust(1.second)
        _          <- latch.await
      } yield ok) // If execution is not parallel, the latch will not be released
    }

  "parallelize single partition handling based on key when using parallel consumer" in
    new ctx(highWatermark = 10) {
      val numKeys = 8
      val keys    = getKeys(numKeys)

      run(for {
        latch      <- CountDownLatch.make(numKeys)
        slowHandler = { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => Clock.sleep(1.second) *> latch.countDown }
        ref        <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        init       <- getInit
        dispatcher <- Dispatcher.make(
                        "group",
                        "clientId",
                        slowHandler,
                        lowWatermark,
                        highWatermark,
                        workersShutdownRef = ref,
                        consumeInParallel = true,
                        maxParallelism = 8,
                        init = init
                      )
        // produce with unique keys to the same partition
        _          <- submitBatch(dispatcher, keys.map(key => record.copy(partition = 0, key = key)))
        _          <- TestClock.adjust(1.second)
        _          <- latch.await
      } yield ok) // if execution is not parallel, the latch will not be released
    }

  "reject records when high watermark is reached" in
    new ctx() {
      run(for {
        ref        <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        init       <- getInit
        dispatcher <-
          Dispatcher
            .make[Any]("group", "clientId", _ => ZIO.never, lowWatermark, highWatermark, workersShutdownRef = ref, init = init)
        _          <- submit(dispatcher, record.copy(offset = 0L)) // Will be polled
        _          <- submit(dispatcher, record.copy(offset = 1L))
        _          <- submit(dispatcher, record.copy(offset = 2L))
        _          <- submit(dispatcher, record.copy(offset = 3L))
        _          <- submit(dispatcher, record.copy(offset = 4L))
        result1    <- submit(dispatcher, record.copy(offset = 5L)) // maybe will be dropped
        result2    <- submit(dispatcher, record.copy(offset = 6L)) // maybe will be dropped
      } yield (result1 must equalTo(SubmitResult.Rejected)) or (result2 must equalTo(SubmitResult.Rejected)))
    }

  "reject records and return first rejected when high watermark is reached on batch submission" in
    new ctx(highWatermark = 5) {
      run(for {
        ref        <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        init       <- getInit
        dispatcher <-
          Dispatcher
            .make[Any]("group", "clientId", _ => ZIO.never, lowWatermark, highWatermark, workersShutdownRef = ref, init = init)
        records     = (0 until 7).map(i => record.copy(offset = i.toLong))
        result     <- submitBatch(dispatcher, records)
      } yield result must beEqualTo(SubmitResult.RejectedBatch(record.copy(offset = 5L))))
    }

  "resume paused partitions" in
    new ctx(lowWatermark = 3, highWatermark = 7) {
      run(
        for {
          queue      <- Queue.bounded[Record](1)
          ref        <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
          init       <- getInit
          dispatcher <- Dispatcher.make[Any](
                          "group",
                          "clientId",
                          record => queue.offer(record).flatMap(result => ZIO.succeed(println(s"queue.offer result: ${result}"))),
                          lowWatermark,
                          highWatermark,
                          workersShutdownRef = ref,
                          init = init
                        )
          _          <- ZIO.foreachDiscard(0 to (highWatermark + 1)) { offset =>
                          submit(
                            dispatcher,
                            ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
                          )
                        }
          _          <- submit(
                          dispatcher,
                          ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 6L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
                        ) // Will be dropped
          _          <- eventuallyZ(dispatcher.resumeablePartitions(Set(topicPartition)))(_.isEmpty)
          _          <- ZIO.foreachDiscard(1 to 4)(_ => queue.take)
          _          <- eventuallyZ(dispatcher.resumeablePartitions(Set(topicPartition)))(_ == Set(TopicPartition(topic, partition)))
        } yield ok
      )
    }

  "block resume paused partitions" in
    new ctx(lowWatermark = 30, highWatermark = 34) {
      run(
        for {
          queue                                   <- Queue.bounded[Record](1)
          ref                                     <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
          init                                    <- getInit
          dispatcher                              <- Dispatcher.make[TestClock](
                                                       "group",
                                                       "clientId",
                                                       record =>
                                                         queue
                                                           .offer(record)
                                                           .flatMap(result => ZIO.succeed(println(s"block resume paused partitions -queue.offer result: ${result}"))),
                                                       lowWatermark,
                                                       highWatermark,
                                                       delayResumeOfPausedPartition = 6500,
                                                       workersShutdownRef = ref,
                                                       init = init
                                                     )
          _                                       <- ZIO.foreachDiscard(0 to (highWatermark + 1)) { offset =>
                                                       submit(
                                                         dispatcher,
                                                         ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
                                                       )
                                                     }
          overCapacitySubmitResult                <-
            submit(
              dispatcher,
              ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 6L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
            ) // Will be dropped
          resumeablePartitionsWhenInHighWatermark <- dispatcher.resumeablePartitions(Set(topicPartition))
          _                                       <- ZIO.foreachDiscard(1 to 4)(_ => queue.take)
          _                                       <- TestClock.adjust(1.second)
          resumablePartitionDuringBlockPeriod     <- dispatcher.resumeablePartitions(Set(topicPartition))
          _                                       <- TestClock.adjust(6.second)
          resumablePartitionAfterBlockPeriod      <- dispatcher.resumeablePartitions(Set(topicPartition))
          _                                       <- ZIO.foreachDiscard(0 to 3) { offset =>
                                                       submit(
                                                         dispatcher,
                                                         ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, offset, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
                                                       )
                                                     }
          overCapacitySubmitResult2               <-
            submit(
              dispatcher,
              ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 16L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)
            ) // Will be dropped
          _                                       <- ZIO.foreachDiscard(1 to 4)(_ => queue.take)
          _                                       <- TestClock.adjust(1.second)
          // test clearPausedPartitionDuration
          resumablePartitionDuringBlockPeriod2    <- dispatcher.resumeablePartitions(Set(topicPartition))
        } yield (resumeablePartitionsWhenInHighWatermark aka "resumeablePartitionsWhenInHighWatermark" must beEmpty) and
          (resumablePartitionDuringBlockPeriod aka "resumablePartitionDuringBlockPeriod" must beEmpty) and
          (resumablePartitionAfterBlockPeriod aka "resumablePartitionAfterBlockPeriod" mustEqual Set(TopicPartition(topic, partition))) and
          (overCapacitySubmitResult aka "overCapacitySubmitResult" mustEqual Rejected) and
          (overCapacitySubmitResult2 aka "overCapacitySubmitResult2" mustEqual Rejected) and
          (resumablePartitionDuringBlockPeriod2 aka "resumablePartitionDuringBlockPeriod2" must beEmpty)
      )

      private def submit(
        dispatcher: Dispatcher[TestClock],
        record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]
      ) =
        dispatcher.submit(record).tap(_ => TestClock.adjust(10.millis))
    }

  "pause handling" in
    new ctx() {
      run(for {
        ref                <- Ref.make(0)
        workersShutdownRef <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        init               <- getInit
        dispatcher         <-
          Dispatcher
            .make[Any](
              "group",
              "clientId",
              _ => ref.update(_ + 1),
              lowWatermark,
              highWatermark,
              workersShutdownRef = workersShutdownRef,
              init = init
            )
        _                  <- pause(dispatcher)
        _                  <- submit(dispatcher, record) // Will be queued
        invocations        <- ref.get
      } yield invocations must equalTo(0))
    }

  "complete executing task before pausing" in
    new ctx() {
      run(for {
        ref                <- Ref.make(0)
        workersShutdownRef <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        promise            <- Promise.make[Nothing, Unit]
        handler             = { _: Record => Clock.sleep(1.second) *> ref.update(_ + 1) *> promise.succeed(()) }
        init               <- getInit
        dispatcher         <-
          Dispatcher
            .make[Any]("group", "clientId", handler, lowWatermark, highWatermark, workersShutdownRef = workersShutdownRef, init = init)
        _                  <- submit(dispatcher, record) // Will be handled
        _                  <- TestMetrics.reported.flatMap(waitUntilRecordHandled(3.seconds))
        _                  <- pause(dispatcher)
        _                  <- submit(dispatcher, record) // Will be queued
        _                  <- TestClock.adjust(1.second)
        _                  <- promise.await
        invocations        <- ref.get
      } yield invocations must equalTo(1))
    }

  "resume handling" in
    new ctx() {
      run(for {
        ref                <- Ref.make(0)
        workersShutdownRef <- Ref.make[Map[TopicPartition, ShutdownPromise]](Map.empty)
        promise            <- Promise.make[Nothing, Unit]
        handler             = { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => ref.update(_ + 1) *> promise.succeed(()) }
        init               <- getInit
        dispatcher         <-
          Dispatcher
            .make("group", "clientId", handler, lowWatermark, highWatermark, workersShutdownRef = workersShutdownRef, init = init)
        _                  <- pause(dispatcher)
        _                  <- submit(dispatcher, record)
        _                  <- resume(dispatcher)
        _                  <- promise.await
        invocations        <- ref.get
      } yield invocations must equalTo(1))
    }

  private def submit(
    dispatcher: Dispatcher[Any],
    record: ConsumerRecord[Chunk[Byte], Chunk[Byte]]
  ): URIO[Env, SubmitResult] = {
    dispatcher.submit(record).tap(_ => TestClock.adjust(10.millis))
  }

  private def submitBatch(
    dispatcher: Dispatcher[Any],
    records: Seq[ConsumerRecord[Chunk[Byte], Chunk[Byte]]]
  ): URIO[Env, SubmitResult] =
    dispatcher.submitBatch(records).tap(_ => TestClock.adjust(10.millis))

  private def waitUntilRecordHandled(timeout: zio.Duration)(metrics: Seq[GreyhoundMetric]) =
    ZIO
      .when(metrics.collect { case r: RecordHandled[_, _] => r }.nonEmpty)(ZIO.fail(TimeoutWaitingForHandledMetric))
      .retry(zio.Schedule.duration(timeout))

  private def resume(dispatcher: Dispatcher[Nothing]) = {
    dispatcher.resume *> TestClock.adjust(10.millis)
  }

  private def pause(dispatcher: Dispatcher[Nothing]) = {
    dispatcher.pause *> TestClock.adjust(10.millis)
  }

  abstract class ctx(val lowWatermark: Int = 2, val highWatermark: Int = 4) extends Scope {
    val topic          = "topic"
    val partition      = 0
    val topicPartition = TopicPartition(topic, partition)
    val record         = ConsumerRecord[Chunk[Byte], Chunk[Byte]](topic, partition, 0L, Headers.Empty, None, Chunk.empty, 0L, 0L, 0L)

    def getKeys(numKeys: Int) = (0 until numKeys).map(i => Some(Chunk.fromArray(s"key$i".getBytes)))

    def getInit() = for {
      init <- Promise.make[Nothing, Unit]
      _    <- init.succeed(())
    } yield init
  }

}

case object TimeoutWaitingForHandledMetric extends GreyhoundMetric
