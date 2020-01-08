package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Consumer.Records
import com.wixpress.dst.greyhound.core.consumer.EventLoopTest._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers.beRecordWithOffset
import com.wixpress.dst.greyhound.core.{Offset, Topic}
import org.apache.kafka.clients.consumer.{ConsumerRecords, ConsumerRecord => KafkaConsumerRecord}
import org.apache.kafka.common.TopicPartition
import zio._
import zio.blocking.Blocking
import zio.duration.Duration

import scala.collection.JavaConverters._

class EventLoopTest extends BaseTest[GreyhoundMetrics with Blocking] {

  type Env = GreyhoundMetrics with Blocking

  override def env: UManaged[Env] =
    Managed.succeed(new GreyhoundMetric.Live with Blocking.Live)

  "subscribe to topics on startup" in {
    for {
      offsets <- Offsets.make
      promise <- Promise.make[Nothing, Set[Topic]]
      consumer = new EmptyConsumer {
        override def subscribe(topics: Set[Topic]): RIO[Blocking, Unit] =
          promise.succeed(topics).unit
      }
      subscribed <- EventLoop.make[Env](
        consumer = consumer,
        offsets = offsets,
        handler = RecordHandler("topic-1", "topic-2")(_ => ZIO.unit)).use_(promise.await)
    } yield subscribed must equalTo(Set("topic-1", "topic-2"))
  }

  "handle polled records" in {
    for {
      offsets <- Offsets.make
      queue <- Queue.unbounded[ConsumerRecord[Chunk[Byte], Chunk[Byte]]]
      consumer = new EmptyConsumer {
        override def poll(timeout: Duration): RIO[Blocking, Records] =
          recordsFrom(
            new KafkaConsumerRecord(topic, 0, 0L, bytes, bytes),
            new KafkaConsumerRecord(topic, 0, 1L, bytes, bytes),
            new KafkaConsumerRecord(topic, 0, 2L, bytes, bytes))
      }
      handled <- EventLoop.make[Env](consumer, offsets, RecordHandler(topic)(queue.offer)).use_ {
        ZIO.collectAll(List.fill(3)(queue.take))
      }
    } yield handled must
      (contain(beRecordWithOffset(0L)) and
        contain(beRecordWithOffset(1L)) and
        contain(beRecordWithOffset(2L)))
  }

  "commit handled records" in {
    for {
      offsets <- Offsets.make
      promise <- Promise.make[Nothing, Map[TopicPartition, Offset]]
      consumer = new EmptyConsumer {
        override def poll(timeout: Duration): RIO[Blocking, Records] =
          recordsFrom(
            new KafkaConsumerRecord(topic, 0, 0L, bytes, bytes),
            new KafkaConsumerRecord(topic, 0, 1L, bytes, bytes),
            new KafkaConsumerRecord(topic, 1, 2L, bytes, bytes))

        override def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking, Unit] =
          promise.succeed(offsets).unit
      }
      committed <- EventLoop.make[Env](consumer, offsets, RecordHandler(topic)(offsets.update)).use_(promise.await)
    } yield committed must havePairs(
      new TopicPartition(topic, 0) -> 1L,
      new TopicPartition(topic, 1) -> 2L)
  }

  "don't commit empty offsets map" in {
    for {
      offsets <- Offsets.make
      promise <- Promise.make[Unit, Unit]
      currentPoll <- Ref.make(0)
      consumer = new EmptyConsumer {
        override def poll(timeout: Duration): RIO[Blocking, Records] =
          currentPoll.update(_ + 1).flatMap {
            // Return empty result on first poll
            case 1 => recordsFrom()
            // Release the promise on the second poll
            case _ => promise.succeed(()) *> recordsFrom()
          }

        override def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking, Unit] =
          promise.fail(()).unit
      }
      result <- EventLoop.make[Env](consumer, offsets, emptyHandler(topic)).use_(promise.await.either)
    } yield result must beRight
  }

  "pause partitions" in {
    val partitions = Set(new TopicPartition(topic, 0))

    for {
      offsets <- Offsets.make
      promise <- Promise.make[Nothing, Set[TopicPartition]]
      consumer = new EmptyConsumer {
        override def pause(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
          promise.succeed(partitions).unit
      }
      paused <- EventLoop.make[Env](consumer, offsets, emptyHandler(topic)).use { eventLoop =>
        eventLoop.pause(partitions) *> promise.await
      }
    } yield paused must equalTo(partitions)
  }

  "resume partitions" in {
    val partitions = Set(new TopicPartition(topic, 0))

    for {
      offsets <- Offsets.make
      promise <- Promise.make[Nothing, Set[TopicPartition]]
      consumer = new EmptyConsumer {
        override def resume(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
          promise.succeed(partitions).unit
      }
      resumed <- EventLoop.make[Env](consumer, offsets, emptyHandler(topic)).use { eventLoop =>
        eventLoop.resume(partitions) *> promise.await
      }
    } yield resumed must equalTo(partitions)
  }

}

object EventLoopTest {
  val topic = "topic"

  val bytes = Chunk.empty

  def emptyHandler(topic: Topic): RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]] =
    RecordHandler(topic)(_ => ZIO.unit)
}

trait EmptyConsumer extends Consumer {
  override def subscribe(topics: Set[Topic]): RIO[Blocking, Unit] =
    ZIO.unit

  override def poll(timeout: Duration): RIO[Blocking, Records] =
    ZIO.succeed(ConsumerRecords.empty())

  override def commit(offsets: Map[TopicPartition, Offset]): RIO[Blocking, Unit] =
    ZIO.unit

  override def pause(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
    ZIO.unit

  override def resume(partitions: Set[TopicPartition]): RIO[Blocking, Unit] =
    ZIO.unit

  def recordsFrom(records: Consumer.Record*): UIO[Consumer.Records] = ZIO.succeed {
    val recordsMap = records.groupBy(record => new TopicPartition(record.topic, record.partition))
    new ConsumerRecords(recordsMap.mapValues(_.asJava).asJava)
  }
}
