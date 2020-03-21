package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Consumer.{Record, Records}
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric._
import com.wixpress.dst.greyhound.core.consumer.EventLoopTest._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, TestMetrics}
import com.wixpress.dst.greyhound.core.{Headers, Offset, Topic}
import org.apache.kafka.clients.consumer.{ConsumerRecords, ConsumerRecord => KafkaConsumerRecord}
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import zio._
import zio.clock.Clock
import zio.duration._

import scala.collection.JavaConverters._

class EventLoopTest extends BaseTest[Clock with TestMetrics] {

  override def env: UManaged[Clock with TestMetrics] =
    TestMetrics.make.map { testMetrics =>
      new TestMetrics with Clock.Live {
        override val metrics: TestMetrics.Service =
          testMetrics.metrics
      }
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
      handled <- EventLoop.make("group", ReportingConsumer(clientId, group, consumer), handler).use_(promise.await)
      metrics <- TestMetrics.reported
    } yield (handled must equalTo(ConsumerRecord(topic, partition, offset, Headers.Empty, None, Chunk.empty, -1L, -2L))) and
      (metrics must contain(PollingFailed(clientId, group, exception)))
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

        override def commit(offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean): RIO[Any, Unit] =
          commitInvocations.update(_ + 1).flatMap {
            case 1 => ZIO.fail(exception)
            case _ => promise.succeed(offsets).unit
          }
      }
      committed <- EventLoop.make("group", ReportingConsumer(clientId, group, consumer), RecordHandler.empty).use_(promise.await)
      metrics <- TestMetrics.reported
    } yield (committed must havePair(TopicPartition(topic, partition) -> (offset + 1))) and
      (metrics must contain(CommitFailed(clientId, group, exception, Map(TopicPartition(record.topic(), record.partition()) -> (offset + 1)))))
  }

  "expose event loop health" in {
    for {
      _ <- ZIO.unit
      sickConsumer = new EmptyConsumer[Any] {
        override def poll(timeout: Duration): Task[Records] =
          ZIO.dieMessage("cough :(")
      }
      died <- EventLoop.make("group", sickConsumer, RecordHandler.empty).use { eventLoop =>
        eventLoop.isAlive.repeat(Schedule.spaced(10.millis) && Schedule.doUntil(alive => !alive)).unit
      }.catchAllCause(_ => ZIO.unit).timeout(1.second)
    } yield died must beSome
  }

}

object EventLoopTest {
  val clientId = "client-id"
  val group = "group"
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
                         rebalanceListener: RebalanceListener[R]): RIO[R, Unit] =
    rebalanceListener.onPartitionsAssigned(topics.map(TopicPartition(_, 0))).unit

  override def poll(timeout: Duration): RIO[R, Records] =
    ZIO.succeed(ConsumerRecords.empty())

  override def commit(offsets: Map[TopicPartition, Offset], calledOnRebalance: Boolean): RIO[R, Unit] =
    ZIO.unit

  override def pause(partitions: Set[TopicPartition]): ZIO[R, IllegalStateException, Unit] =
    ZIO.unit

  override def resume(partitions: Set[TopicPartition]): ZIO[R, IllegalStateException, Unit] =
    ZIO.unit

  override def seek(partition: TopicPartition, offset: Offset): ZIO[R, IllegalStateException, Unit] =
    ZIO.unit
}
