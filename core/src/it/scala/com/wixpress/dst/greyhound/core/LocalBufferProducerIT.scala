package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.{H2LocalBuffer, LocalBuffer, LocalBufferError, LocalBufferFull, LocalBufferProducerConfig}
import com.wixpress.dst.greyhound.core.producer.buffered.{LocalBufferProduceAttemptFailed, LocalBufferProducer}
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, TestMetrics, eventuallyTimeout, eventuallyZ}
import com.wixpress.dst.greyhound.testkit.ITEnv.ManagedKafkaOps
import com.wixpress.dst.greyhound.testkit.{ITEnv, ManagedKafka, ManagedKafkaConfig}
import org.apache.kafka.common.errors.RecordTooLargeException
import zio.Schedule.recurs
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.util.Random

class LocalBufferProducerIT extends BaseTestWithSharedEnv[ITEnv.Env, BufferTestResources] {
  sequential

  override def env: UManaged[ITEnv.Env] =
    for {
      env <- (GreyhoundMetrics.liveLayer ++ test.environment.liveEnvironment).build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics


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
      test <- makeProducer(producer, maxConcurrency = 1).use { localBufferProducer =>
        for {
          queue <- Queue.unbounded[ConsumerRecord[String, String]]
          handler = RecordHandler(queue.offer).withDeserializers(StringSerde, StringSerde)
          record = ProducerRecord(topic, "bar", Some("foo"))
          _ <- RecordConsumer.make(configFor(kafka, "group123", topic), handler).use_ {
            localBufferProducer.produce(record, StringSerde, StringSerde) *>
              eventuallyZ(queue.takeUpTo(100))(_.nonEmpty)
          }
        } yield ok
      }
    } yield test
  }

  "produce in order of per key" in {
    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(prefix = s"buffered-2")

      maxConcurrency = 10
      test <- makeProducer(producer, maxConcurrency).use { localBufferProducer =>
        for {
          consumed <- Ref.make(Map.empty[String, Seq[Int]])
          handler = RecordHandler(putIn(consumed)).withDeserializers(StringSerde, IntSerde)
          record = ProducerRecord(topic, 0)
          (keyCount, recordPerKey) = (100, 20)
          _ <- RecordConsumer.make(configFor(kafka, "group234", topic), handler).use_ {
            produceMultiple(keyCount, recordPerKey)(localBufferProducer, record) *>
              eventuallyTimeout(consumed.get)(_ == expectedMap(recordPerKey, keyCount))(20.seconds)
          }
          state <- localBufferProducer.currentState

          queryCountAfterComplete = state.localBufferQueryCount
          queryCountAfterDelay <- localBufferProducer.currentState.delay(1.second).map(_.localBufferQueryCount)
        } yield (
          (state.maxRecordedConcurrency === maxConcurrency) and
            (queryCountAfterDelay === queryCountAfterComplete) and
            (queryCountAfterComplete must beGreaterThan(1)))
      }
    } yield test
  }

  "allow waiting on kafka record sent" in {
    def produceIO(topic: Topic, producer: LocalBufferProducer) =
      producer.produce(ProducerRecord(topic, 0), StringSerde, IntSerde)
        .tap(res => UIO(println("produced to local id: " + res.localMessageId)))
        .flatMap(_.kafkaResult.await)
        .timeoutFail(LocalBufferError(TimeoutProducingRecord))(10.seconds)

    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(prefix = s"buffered-3", partitions = 1)

      test <- makeProducer(producer, maxConcurrency = 10).use { localBufferProducer =>
        for {
          kafkaResult1 <- produceIO(topic, localBufferProducer)
          kafkaResult2 <- produceIO(topic, localBufferProducer)
        } yield (kafkaResult1 === RecordMetadata(topic, partition = 0, offset = 0L) and
          kafkaResult2 === RecordMetadata(topic, partition = 0, offset = 1L))
      }} yield test
  }

  "keep retrying on retriable errors" in {
    def record(topic: Topic) = ProducerRecord(topic, "0")

    for {
      BufferTestResources(kafka, _) <- getShared
      test <- Producer.make(failFastInvalidBrokersConfig).use { producer =>
        (for {
          topic <- kafka.createRandomTopic(prefix = s"buffered-4", partitions = 1)
          (timeoutCount, state) <- makeProducer(producer, maxConcurrency = 10, flushTimeout = 1.second).use { localBufferProducer =>
            for {
              _ <- localBufferProducer.produce(record(topic), IntSerde, StringSerde).fork
              _ <- localBufferProducer.produce(record(topic), IntSerde, StringSerde).flatMap(_.kafkaResult.await).timeout(10.second)
              timeouts <- TestMetrics.reported.map(_.collect { case e@LocalBufferProduceAttemptFailed(TimeoutError(_), false) => e })
              state <- localBufferProducer.currentState
            } yield (timeouts.size, state)
          }
        } yield ((timeoutCount must beGreaterThan(1))) and (state.inflight === 2) and (state.enqueued === 0))
      }
    } yield test
  }

  "not retry on unretriable errors" in {
    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(1, params = Map("max.message.bytes" -> "100"))
      record = ProducerRecord(topic, value = Random.alphanumeric.take(300).mkString)
      test <- makeProducer(producer, maxConcurrency = 1).use { localBufferProducer =>
        for {
          producerError <- localBufferProducer.produce(record, IntSerde, StringSerde).flatMap(_.kafkaResult.await.flip)
          metrics <- TestMetrics.reported
          reportedNonRetriableErrors = metrics.collect { case s@LocalBufferProduceAttemptFailed(KafkaError(_), true) => s }
          state <- localBufferProducer.currentState
        } yield
          (reportedNonRetriableErrors.size === 1) and
            (state.failedRecords === 1) and
            (producerError.getCause.getClass === classOf[RecordTooLargeException])
      }
    } yield test
  }

  "throw exceptions when persistent buffer gets filled" in {
    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(1)
      record = ProducerRecord(topic, value = "0")
      test <- makeProducer(producer, maxConcurrency = 1, maxMessagesOnDisk = 10).use { localBufferProducer =>
        localBufferProducer.produce(record, IntSerde, StringSerde).repeat(recurs(999)).flip
          .map(_ === LocalBufferError(LocalBufferFull(10)))
      }
    } yield test
  }

  "not try to send messages if their submit time is older than configured timeout" in {
    for {
      BufferTestResources(kafka, _) <- getShared
      result <- Producer.make(failFastInvalidBrokersConfig).use { producer =>
        for {
          topic <- kafka.createRandomTopic(1)
          record = ProducerRecord(topic, value = "0")
          test <- makeProducer(producer, maxConcurrency = 1, giveUpAfter = 10.millis).use { localBufferProducer =>
            for {
              produceError <- localBufferProducer.produce(record, IntSerde, StringSerde).flatMap(_.kafkaResult.await).repeat(recurs(1000)).either
              _ <- localBufferProducer.shutdown
              produceAfterShutdown <- localBufferProducer.produce(record, IntSerde, StringSerde).flip
            } yield (produceError.left.get.getClass === classOf[TimeoutError]) and (produceAfterShutdown.cause.getClass === classOf[ProducerClosed])
          }
        } yield test
      }
    } yield result
  }

  "on shutdown flush all inflight messages" in {
    for {
      BufferTestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(1)
      record = ProducerRecord(topic, value = "0")
      results <- makeProducer(producer, maxConcurrency = 1).use { localBufferProducer =>
        ZIO.foreach(0 until 1000)(i =>
          localBufferProducer.produce(record.copy(key = Some(i)), IntSerde, StringSerde)
        )
      }
      // producer is shutdown out of managed scope - checking that the promises are still fulfilled eventually
      recordsProduced <- ZIO.foreach(results)(_.kafkaResult.await).timeoutFail(new RuntimeException("TIMEOUT!"))(15.seconds)
    } yield recordsProduced.size === 1000
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
    record =>
      consumed.update(map => map + (record.key.get -> (map.getOrElse(record.key.get, Nil) :+ record.value)))

  private def makeProducer(producer: Producer,
                           maxConcurrency: Partition, maxMessagesOnDisk: Int = 10000,
                           giveUpAfter: Duration = 1.day,
                           flushTimeout: Duration = 1.minute,
                           retryInterval: Duration = 1.second): ZManaged[ZEnv with GreyhoundMetrics, Throwable, LocalBufferProducer] =
    makeH2Buffer.flatMap(buffer =>
      LocalBufferProducer.make(producer, buffer, LocalBufferProducerConfig(maxConcurrency = maxConcurrency,
        maxMessagesOnDisk = maxMessagesOnDisk, giveUpAfter = giveUpAfter, shutdownFlushTimeout = flushTimeout,
        retryInterval = retryInterval)))

  private def makeH2Buffer: RManaged[Clock, LocalBuffer] = H2LocalBuffer.make(s"./tests-data/test-producer-${Math.abs(Random.nextInt(100000))}", keepDeadMessages = 1.day)

  private def configFor(kafka: ManagedKafka, group: Group, topic: Topic) = RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic)), extraProperties = fastConsumerMetadataFetching, offsetReset = OffsetReset.Earliest)

  private def fastConsumerMetadataFetching = Map("metadata.max.age.ms" -> "0")

  private def failFastInvalidBrokersConfig = ProducerConfig("localhost:27461", ProducerRetryPolicy(0, 0.millis), Map("max.block.ms" -> "0"))

}

case class BufferTestResources(kafka: ManagedKafka, producer: Producer)

object TimeoutProducingRecord extends RuntimeException
