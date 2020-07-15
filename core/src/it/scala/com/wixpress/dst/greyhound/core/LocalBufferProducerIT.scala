package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.buffered.LocalBufferProducer
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.{H2LocalBuffer, LocalBuffer, LocalBufferError, LocalBufferProducerConfig}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord, RecordMetadata, ReportingProducer}
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, eventuallyTimeout, eventuallyZ}
import com.wixpress.dst.greyhound.testkit.ITEnv.ManagedKafkaOps
import com.wixpress.dst.greyhound.testkit.{ITEnv, ManagedKafka, ManagedKafkaConfig}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.util.Random

class LocalBufferProducerIT extends BaseTestWithSharedEnv[ITEnv.Env, BufferTestResources] {
  sequential

  override def env: UManaged[ITEnv.Env] =
    (GreyhoundMetrics.liveLayer ++ test.environment.liveEnvironment).build

  override def sharedEnv: ZManaged[Blocking with GreyhoundMetrics, Throwable, BufferTestResources] = resources

  val resources: ZManaged[Blocking with GreyhoundMetrics, Throwable, BufferTestResources] =
    for {
      kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
      producer <- Producer.make(ProducerConfig(kafka.bootstrapServers)).map(p => ReportingProducer(p))
    } yield BufferTestResources(kafka, producer)

  "produce and consume via local buffer" in {
    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(prefix = s"buffered-1")
      localBuffer <- makeH2Buffer
      localBufferProducer <- makeProducer(producer, localBuffer, maxConcurrency = 1)
      queue <- Queue.unbounded[ConsumerRecord[String, String]]
      handler = RecordHandler(queue.offer).withDeserializers(StringSerde, StringSerde)
      record = ProducerRecord(topic, "bar", Some("foo"))
      _ <- RecordConsumer.make(configFor(kafka, "group123", topic), handler).use_ {
        localBufferProducer.produce(record, StringSerde, StringSerde) *>
          eventuallyZ(queue.takeUpTo(100))(_.nonEmpty)
      }
    } yield ok
  }

  "produce in order of per key" in {
    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(prefix = s"buffered-2")
      localBuffer <- makeH2Buffer
      maxConcurrency = 10
      localBufferProducer <- makeProducer(producer, localBuffer, maxConcurrency)
      consumed <- Ref.make(Map.empty[String, Seq[Int]])
      handler = RecordHandler(putIn(consumed)).withDeserializers(StringSerde, IntSerde)
      record = ProducerRecord(topic, 0)
      (keyCount, recordPerKey) = (100, 20)
      _ <- RecordConsumer.make(configFor(kafka, "group234", topic), handler).use_ {
        produceMultiple(keyCount, recordPerKey)(localBufferProducer, record) *>
          eventuallyTimeout(consumed.get)(_ == expectedMap(recordPerKey, keyCount))(10.seconds)
      }
      activeFibers <- localBufferProducer.currentState.map(_.maxRecordedConcurrency)
    } yield activeFibers === maxConcurrency
  }

  "allow waiting on kafka record sent" in {
    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(prefix = s"buffered-3", partitions = 1)
      localBuffer <- makeH2Buffer
      localBufferProducer <- makeProducer(producer, localBuffer, maxConcurrency = 10)
      record = ProducerRecord(topic, 0)
      produceIO = localBufferProducer.produce(record, StringSerde, IntSerde).flatMap(_.kafkaResult)
        .timeoutFail(LocalBufferError(TimeoutProducingRecord))(10.seconds)
      kafkaResult1 <- produceIO
      kafkaResult2 <- produceIO
    } yield (kafkaResult1 === RecordMetadata(topic, partition = 0, offset = 0L) and
      kafkaResult2 === RecordMetadata(topic, partition = 0, offset = 1L))
  }

  private def produceMultiple(keyCount: Int, recordPerKey: Int)(localBufferProducer: LocalBufferProducer, record: ProducerRecord[String, Int]) =
    ZIO.foreach(0 until (keyCount * recordPerKey)) { i =>
      localBufferProducer.produce(record.copy(value = i, key = Some((i % keyCount).toString)), StringSerde, IntSerde)
    }

  private def expectedMap(recordPerKey: Int, keyCount: Int): Map[String, Seq[Int]] =
    (0 until keyCount).map(key => key.toString -> expectedListForKey(key, recordPerKey, keyCount)).toMap

  private def expectedListForKey(key: Int, recordPerKey: Int, keyCount: Int): Seq[Int] =
    (0 until recordPerKey).map(i => keyCount * i + key)

  private def putIn(consumed: Ref[Map[String, Seq[Int]]]): ConsumerRecord[String, Int] => UIO[Unit] =
    record => consumed.update(map => map + (record.key.get -> (map.getOrElse(record.key.get, Nil) :+ record.value)))

  private def makeProducer(producer: Producer, localBuffer: LocalBuffer, maxConcurrency: Partition) =
    LocalBufferProducer.make(producer, localBuffer, LocalBufferProducerConfig(maxConcurrency = maxConcurrency, maxMessagesOnDisk = 10000, giveUpAfter = 1.day))

  private def makeH2Buffer: RIO[Clock, LocalBuffer] =
    H2LocalBuffer.make(s"./tests-data/test-producer-${Math.abs(Random.nextInt(100000))}", keepDeadMessages = 1.day)

  private def configFor(kafka: ManagedKafka, group: Group, topic: Topic) =
    RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic)), extraProperties = fastConsumerMetadataFetching)

  private def fastConsumerMetadataFetching =
    Map("metadata.max.age.ms" -> "0")
}

case class BufferTestResources(kafka: ManagedKafka, producer: Producer)

object TimeoutProducingRecord extends RuntimeException