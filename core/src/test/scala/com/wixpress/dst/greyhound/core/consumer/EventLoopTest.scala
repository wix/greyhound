package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Consumer.{RebalanceListener, Records}
import com.wixpress.dst.greyhound.core.consumer.EventLoopTest._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers.beRecordWithOffset
import com.wixpress.dst.greyhound.core.{Offset, Topic}
import org.apache.kafka.clients.consumer.{ConsumerRecords, ConsumerRecord => KafkaConsumerRecord}
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.collection.JavaConverters._

class EventLoopTest extends BaseTest[Env] {

  override def env: UManaged[Env] =
    Managed.succeed(new GreyhoundMetric.Live with Clock.Live with Blocking.Live)

  "subscribe to topics on startup" in {
    for {
      offsets <- Offsets.make
      promise <- Promise.make[Nothing, Set[Topic]]
      consumer = new EmptyConsumer[Env] {
        override def subscribe(topics: Set[Topic],
                               onPartitionsRevoked: RebalanceListener[Env],
                               onPartitionsAssigned: RebalanceListener[Env]): RIO[Env, Unit] =
          promise.succeed(topics).unit *>
            super.subscribe(topics, onPartitionsRevoked, onPartitionsAssigned)
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
      consumer = new EmptyConsumer[Env] {
        override def poll(timeout: Duration): RIO[Env, Records] =
          recordsFrom(
            new KafkaConsumerRecord(topic, 0, 0L, bytes, bytes),
            new KafkaConsumerRecord(topic, 0, 1L, bytes, bytes),
            new KafkaConsumerRecord(topic, 0, 2L, bytes, bytes))
      }
      handler = RecordHandler(topic)(queue.offer(_: ConsumerRecord[Chunk[Byte], Chunk[Byte]]).unit)
      handled <- EventLoop.make[Env](consumer, offsets, handler.andThen(offsets.update)).use_ {
        ZIO.collectAll(List.fill(3)(queue.take))
      }
    } yield handled must
      (contain(beRecordWithOffset(0L)) and
        contain(beRecordWithOffset(1L)) and
        contain(beRecordWithOffset(2L)))
  }

  "commit handled offsets + 1" in {
    for {
      offsets <- Offsets.make
      promise <- Promise.make[Nothing, Map[TopicPartition, Offset]]
      consumer = new EmptyConsumer[Env] {
        override def poll(timeout: Duration): RIO[Env, Records] =
          recordsFrom(
            new KafkaConsumerRecord(topic, 0, 0L, bytes, bytes),
            new KafkaConsumerRecord(topic, 0, 1L, bytes, bytes),
            new KafkaConsumerRecord(topic, 1, 2L, bytes, bytes))

        override def commit(offsets: Map[TopicPartition, Offset]): UIO[Unit] =
          promise.succeed(offsets).unit
      }
      committed <- EventLoop.make[Env](consumer, offsets, RecordHandler(topic)(offsets.update)).use_(promise.await)
    } yield committed must havePairs(
      TopicPartition(topic, 0) -> 2L,
      TopicPartition(topic, 1) -> 3L)
  }

  "don't commit empty offsets map" in {
    for {
      offsets <- Offsets.make
      promise <- Promise.make[Unit, Unit]
      currentPoll <- Ref.make(0)
      consumer = new EmptyConsumer[Env] {
        override def poll(timeout: Duration): RIO[Env, Records] =
          currentPoll.update(_ + 1).flatMap {
            // Return empty result on first poll
            case 1 => recordsFrom()
            // Release the promise on the second poll
            case _ => promise.succeed(()) *> recordsFrom()
          }

        override def commit(offsets: Map[TopicPartition, Offset]): UIO[Unit] =
          promise.fail(()).unit
      }
      result <- EventLoop.make[Env](consumer, offsets, emptyHandler(topic)).use_(promise.await.either)
    } yield result must beRight
  }

}

object EventLoopTest {
  type Env = GreyhoundMetrics with Clock with Blocking

  val topic = "topic"

  val bytes = Chunk.empty

  def emptyHandler(topic: Topic): RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]] =
    RecordHandler(topic)(_ => ZIO.unit)
}

trait EmptyConsumer[R] extends Consumer[R] {
  override def subscribe(topics: Set[Topic],
                         onPartitionsRevoked: RebalanceListener[R],
                         onPartitionsAssigned: RebalanceListener[R]): RIO[R, Unit] =
    onPartitionsAssigned(topics.map(TopicPartition(_, 0)))

  override def poll(timeout: Duration): RIO[R, Records] =
    ZIO.succeed(ConsumerRecords.empty())

  override def commit(offsets: Map[TopicPartition, Offset]): RIO[R, Unit] =
    ZIO.unit

  override def pause(partitions: Set[TopicPartition]): RIO[R, Unit] =
    ZIO.unit

  override def resume(partitions: Set[TopicPartition]): RIO[R, Unit] =
    ZIO.unit

  override def seek(partition: TopicPartition, offset: Offset): RIO[R, Unit] =
    ZIO.unit

  override def partitionsFor(topic: Topic): RIO[R, Set[TopicPartition]] =
    ZIO.succeed(Set.empty)

  def recordsFrom(records: Consumer.Record*): RIO[R, Consumer.Records] = ZIO.succeed {
    val recordsMap = records.groupBy(record => new KafkaTopicPartition(record.topic, record.partition))
    new ConsumerRecords(recordsMap.mapValues(_.asJava).asJava)
  }
}
