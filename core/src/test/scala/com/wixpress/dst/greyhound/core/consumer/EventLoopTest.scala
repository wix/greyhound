package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Consumer.{RebalanceListener, Record, Records}
import com.wixpress.dst.greyhound.core.consumer.EventLoopTest._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, TestMetrics}
import com.wixpress.dst.greyhound.core.{Headers, Offset, Topic}
import org.apache.kafka.clients.consumer.{ConsumerRecords, ConsumerRecord => KafkaConsumerRecord}
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import zio._
import zio.duration.Duration
import zio.test.environment.{TestClock, TestEnvironment}

import scala.collection.JavaConverters._

class EventLoopTest extends BaseTest[TestClock with TestMetrics] {

  override def env: UManaged[TestClock with TestMetrics] =
    for {
      env <- TestEnvironment.Value
      testMetrics <- TestMetrics.make
    } yield new TestClock with TestMetrics {
      override val clock: TestClock.Service[Any] = env.clock
      override val scheduler: TestClock.Service[Any] = env.scheduler
      override val metrics: TestMetrics.Service = testMetrics.metrics
    }

  "recover from consumer failing to poll" in {
    for {
      invocations <- Ref.make(0)
      consumer = new EmptyConsumer[Any] {
        override def poll(timeout: Duration): Task[Records] =
          invocations.update(_ + 1).flatMap {
            case 1 => ZIO.fail(exception)
            case 2 => ZIO.succeed(recordsFrom(record))
            case _ => ZIO.succeed(ConsumerRecords.empty())
          }
      }
      promise <- Promise.make[Nothing, ConsumerRecord[Chunk[Byte], Chunk[Byte]]]
      handler = RecordHandler(topic)(promise.succeed)
      handled <- EventLoop.make(ReportingConsumer(consumer), handler).use_(promise.await)
      metrics <- TestMetrics.reported
    } yield (handled must equalTo(ConsumerRecord(topic, partition, offset, Headers.Empty, None, Chunk.empty))) and
      (metrics must contain(PollingFailed(exception)))
  }

  "recover from consumer failing to commit" in {
    for {
      pollInvocations <- Ref.make(0)
      commitInvocations <- Ref.make(0)
      promise <- Promise.make[Nothing, Map[TopicPartition, Offset]]
      consumer = new EmptyConsumer[Any] {
        override def poll(timeout: Duration): Task[Records] =
          pollInvocations.update(_ + 1).flatMap {
            case 1 => ZIO.succeed(recordsFrom(record))
            case _ => ZIO.succeed(ConsumerRecords.empty())
          }

        override def commit(offsets: Map[TopicPartition, Offset]): RIO[Any, Unit] =
          commitInvocations.update(_ + 1).flatMap {
            case 1 => ZIO.fail(exception)
            case _ => promise.succeed(offsets).unit
          }
      }
      committed <- EventLoop.make(ReportingConsumer(consumer), RecordHandler.empty).use_(promise.await)
      metrics <- TestMetrics.reported
    } yield (committed must havePair(TopicPartition(topic, partition) -> (offset + 1))) and
      (metrics must contain(CommitFailed(exception)))
  }

}

object EventLoopTest {
  val topic = "topic"
  val partition = 0
  val offset = 0L
  val record: Record = new KafkaConsumerRecord(topic, partition, offset, null, Chunk.empty)
  val exception = new RuntimeException("oops")

  def recordsFrom(records: Record*): Records = {
    val recordsMap = records.groupBy(record => new KafkaTopicPartition(record.topic, record.partition))
    new ConsumerRecords(recordsMap.mapValues(_.asJava).asJava)
  }
}

trait EmptyConsumer[R] extends Consumer[R] {
  override def subscribe(topics: Set[Topic],
                         onPartitionsRevoked: RebalanceListener[R],
                         onPartitionsAssigned: RebalanceListener[R]): RIO[R, Unit] =
    onPartitionsAssigned(topics.map(TopicPartition(_, 0))).unit

  override def poll(timeout: Duration): RIO[R, Records] =
    ZIO.succeed(ConsumerRecords.empty())

  override def commit(offsets: Map[TopicPartition, Offset]): RIO[R, Unit] =
    ZIO.unit

  override def pause(partitions: Set[TopicPartition]): ZIO[R, IllegalStateException, Unit] =
    ZIO.unit

  override def resume(partitions: Set[TopicPartition]): ZIO[R, IllegalStateException, Unit] =
    ZIO.unit

  override def seek(partition: TopicPartition, offset: Offset): ZIO[R, IllegalStateException, Unit] =
    ZIO.unit
}
