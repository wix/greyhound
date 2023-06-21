package com.wixpress.dst.greyhound.core.consumer.batched

import com.wixpress.dst.greyhound.core.{Offset, OffsetAndMetadata, Topic, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.consumer.batched.BatchConsumer.RecordBatch
import com.wixpress.dst.greyhound.core.consumer.batched.BatchEventLoopMetric.{FullBatchHandled, RecordsHandled}
import com.wixpress.dst.greyhound.core.consumer.batched.TestSupport._
import com.wixpress.dst.greyhound.core.consumer.domain.{BatchRecordHandler, ConsumerRecord, ConsumerSubscription, HandleError}
import com.wixpress.dst.greyhound.core.consumer.{Consumer, DelayedRebalanceEffect, EmptyConsumer}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.testkit.TestMetrics
import com.wixpress.dst.greyhound.core.testkit.{AwaitableRef, TestCtx}

import zio.test.Assertion._
import zio.test._
import zio.test.junit.JUnitRunnableSpec
import zio.{Cause, Chunk, IO, Queue, RIO, Ref, Task, ZIO}
import zio._

class BatchEventLoopTest extends JUnitRunnableSpec {
  private implicit val trace = zio.Trace.empty

  def spec = suite("BatchEventLoopTest")(
    test("successfully consume a batch of messages and commit offsets") {
      inCtx { c =>
        import c._
        val consumerRecords = records()
        val topics          = consumerRecords.map(_.topic).distinct
        val topicPartitions = consumerRecords.map(_.topicPartition).distinct
        ZIO.scoped(BatchEventLoop.make(group, ConsumerSubscription.topics(topics: _*), consumer, handler, clientId).flatMap { _ =>
          for {
            _              <- givenRecords(consumerRecords)
            handledRecords <- handled.await(_.size >= topicPartitions.size)
            offsets        <- committedOffsetsRef.await(_.size >= topicPartitions.size)
            metrics        <- TestMetrics.reported
          } yield {
            assert(handledRecords)(containsRecordsByPartition(consumerRecords)) &&
            assert(offsets)(equalTo(consumerRecords.groupBy(_.topicPartition).mapValues(_.map(_.offset + 1).max))) &&
            assert(metrics)(exists(isRecordsHandledMetric(topics, group, clientId))) &&
            assert(metrics)(exists(isBatchHandledMetric(group, clientId)))
          }
        })
      }
    },
    test("drop failed topic-partition records, if no retry") {
      checkAll(noRetry) {
        case (retry, cause) =>
          inCtx { c =>
            import c._
            val consumerRecords = records(topicCount = 1, partitions = 2)
            val topics          = consumerRecords.map(_.topic).distinct
            ZIO.scoped(BatchEventLoop.make(group, ConsumerSubscription.topics(topics: _*), consumer, handler, clientId, retry).flatMap {
              loop =>
                for {
                  _              <- ZIO.debug(s"Should not retry for retry: $retry, cause: $cause")
                  _              <- givenHandleError(failOnPartition(0, cause))
                  _              <- givenRecords(consumerRecords)
                  handledRecords <- handled.await(_.nonEmpty)
                  offsets        <- committedOffsetsRef.await(_.nonEmpty)
                  _              <- TestClock.adjust(500.millis)
                  state          <- loop.state
                } yield {
                  assert(handledRecords)(equalTo(Vector(consumerRecords.filterNot(_.partition == 0)))) &&
                  assert(offsets)(equalTo(consumerRecords.groupBy(_.topicPartition).mapValues(_.map(_.offset + 1).max))) &&
                  assert(state.pendingRecords.values.sum)(equalTo(0))
                }
            })
          }
      }
    },
    test("retry on failed topic-partition records, if retry configured") {
      checkAll(retryCauses) { cause =>
        inCtx { c =>
          import c._
          val consumerRecords = records(topicCount = 1, partitions = 2)
          val topics          = consumerRecords.map(_.topic).distinct
          val retry           = BatchRetryConfig(backoff = 1.second)

          ZIO.scoped(BatchEventLoop.make(group, ConsumerSubscription.topics(topics: _*), consumer, handler, clientId, Some(retry)).flatMap {
            loop =>
              for {
                _        <- ZIO.debug(s"Should retry for cause: $cause")
                _        <- givenHandleError(failOnPartition(0, cause))
                _        <- givenRecords(consumerRecords)
                handled1 <- handled.await(_.nonEmpty)
                offsets1 <- committedOffsetsRef.await(_.nonEmpty)
                _        <- TestClock.adjust(500.millis)
                state1   <- loop.state
                _        <- givenHandleError(_ => None)

                _ <- TestClock.adjust(retry.backoff + 1.milli)

                handled2 <- handled.await(_.size > 1)
                offsets2 <- committedOffsetsRef.await(_.size > 1)
                state2   <- loop.state
              } yield {
                val goodRecords = consumerRecords.filterNot(_.partition == 0)
                assert(handled1)(equalTo(Vector(goodRecords))) &&
                assert(offsets1)(equalTo(goodRecords.groupBy(_.topicPartition).mapValues(_.map(_.offset + 1).max))) &&
                assert(state1.pendingRecords.values.sum)(equalTo(goodRecords.size)) &&
                // -- after partition 0 handling succeeds
                assert(handled2)(containsRecordsByPartition(consumerRecords)) &&
                assert(offsets2)(equalTo(consumerRecords.groupBy(_.topicPartition).mapValues(_.map(_.offset + 1).max))) &&
                assert(state2.pendingRecords.values.sum)(equalTo(0))
              }
          })
        }
      }
    }
  ).provideEnvironment(environment)

  private def environment: ZEnvironment[TestMetrics with Scope with TestEnvironment] =
    TestMetrics.buildEnv ++
      zio.Unsafe.unsafe { implicit s =>
        runtime.unsafe
          .run(
            ZIO.scoped((zio.test.testEnvironment.>+>(TestClock.default) ++ ZLayer.succeed(Scope.global: Scope)).build)
          )
          .getOrThrowFiberFailure()
      }

  private def isRecordsHandledMetric(topics: Seq[Topic], group: String, clientId: String) =
    isSubtype[RecordsHandled[_, _]](
      hasField("group", (_: RecordsHandled[_, _]).group, equalTo(group)) &&
        hasField("topic", (_: RecordsHandled[_, _]).topic, equalTo(topics.head)) &&
        hasField("clientId", (_: RecordsHandled[_, _]).clientId, equalTo(clientId))
    )

  private def isBatchHandledMetric(group: String, clientId: String) =
    isSubtype[FullBatchHandled[_, _]](
      hasField("group", (_: FullBatchHandled[_, _]).group, equalTo(group)) &&
        hasField("clientId", (_: FullBatchHandled[_, _]).clientId, equalTo(clientId))
    )

  type TriggerErrors = Seq[ConsumerRecord[Chunk[Byte], Chunk[Byte]]] => Option[Cause[HandleError[Throwable]]]

  val inCtx = TestCtx {
    for {
      h             <- AwaitableRef.make(Vector.empty[Seq[Consumer.Record]])
      queue         <- Queue.unbounded[Seq[Consumer.Record]]
      offsets       <- AwaitableRef.make(Map.empty[TopicPartition, Offset])
      handlerErrors <- Ref.make[TriggerErrors](_ => None)
    } yield new ctx(h, queue, offsets, handlerErrors)
  }

  class ctx(
    val handled: AwaitableRef[Vector[Seq[Consumer.Record]]],
    queue: Queue[Seq[Consumer.Record]],
    val committedOffsetsRef: AwaitableRef[Map[TopicPartition, Offset]],
    handlerErrorsRef: Ref[TriggerErrors]
  ) {
    val group, clientId = randomStr

    val consumer = new EmptyConsumer {
      override def poll(timeout: Duration)(implicit trace: Trace): Task[Records] =
        queue.take.interruptible
          .timeout(timeout)
          .map(_.getOrElse(Iterable.empty))
          .tap(r => ZIO.debug(s"poll($timeout): $r"))


      override def commit(offsets: Map[TopicPartition, Offset])(implicit trace: Trace): Task[Unit] = {
        ZIO.debug(s"commit($offsets)") *> committedOffsetsRef.update(_ ++ offsets)
      }

      override def commitWithMetadata(offsetsAndMetadata: Map[TopicPartition, OffsetAndMetadata])(
        implicit trace: Trace
      ): RIO[GreyhoundMetrics, Unit] =
        committedOffsetsRef.update(_ ++ offsetsAndMetadata.map { case (tp, om) => tp -> om.offset })

      override def commitOnRebalance(
        offsets: Map[TopicPartition, Offset]
      )(implicit trace: Trace): RIO[GreyhoundMetrics, DelayedRebalanceEffect] = {
        ZIO.runtime[Any].flatMap { rt =>
          ZIO.succeed(
            DelayedRebalanceEffect(
              zio.Unsafe.unsafe { implicit s => rt.unsafe.run(committedOffsetsRef.update(_ ++ offsets)).getOrThrowFiberFailure() }
            )
          )
        }
      }

      override def commitWithMetadataOnRebalance(
        offsets: Map[TopicPartition, OffsetAndMetadata]
      )(implicit trace: Trace): RIO[GreyhoundMetrics, DelayedRebalanceEffect] = {
        ZIO.runtime[Any].flatMap { rt =>
          ZIO.succeed(DelayedRebalanceEffect(zio.Unsafe.unsafe { implicit s =>
                rt.unsafe.run(committedOffsetsRef.update(_ ++ offsets.mapValues(_.offset))).getOrThrowFiberFailure()
              }))
        }
      }
    }

    val handler = new BatchRecordHandler[Any, Throwable, Chunk[Byte], Chunk[Byte]] {
      override def handle(records: RecordBatch): ZIO[Any, HandleError[Throwable], Any] = for {
        _  <- ZIO.debug(s"handle($records)")
        he <- handlerErrorsRef.get
        _  <- he(records.records).fold(ZIO.unit: IO[HandleError[Throwable], Unit])(ZIO.failCause(_))
        _  <- handled
                .update(_ :+ records.records)
                .tapErrorCause(e => ZIO.debug(s"handle failed with $e, records: $records"))
                .tap(_ => ZIO.debug(s"handled $records"))
      } yield ()
    }

    def givenRecords(records: Seq[Consumer.Record]) = queue.offer(records)

    def givenHandleError(trigger: TriggerErrors) = {
      handlerErrorsRef.set(trigger)
    }

    def failOnPartition(
      partition: Int,
      error: Cause[HandleError[Throwable]] = Cause.fail(HandleError(new RuntimeException("kaboom")))
    ): TriggerErrors = recs => recs.find(_.partition == partition).map(_ => error)
  }

  def retryCauses = Gen.fromIterable(Seq(Cause.fail(HandleError(new RuntimeException("fail"))), Cause.die(new RuntimeException("die"))))

  def noRetry = retryCauses.flatMap(cause =>
    Gen.fromIterable(
      Seq(
        Some(BatchRetryConfig(backoff = 2.second)) -> Cause.fail(HandleError(new RuntimeException, forceNoRetry = true)),
        None                                       -> cause
      )
    )
  )
}
