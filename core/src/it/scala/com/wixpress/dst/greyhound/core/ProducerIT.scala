package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.Serdes.{IntSerde, StringSerde}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerError, ProducerRecord, ProducerRetryPolicy, RecordMetadata}
import com.wixpress.dst.greyhound.core.testkit.BaseTestWithSharedEnv
import com.wixpress.dst.greyhound.testkit.ITEnv
import com.wixpress.dst.greyhound.testkit.ITEnv._
import zio._
import zio.duration._

class ProducerIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env: UManaged[ITEnv.Env] = ITEnv.ManagedEnv

  override def sharedEnv = ITEnv.testResources()

  val resources = testResources()

  "produce async" in {
    for {
      TestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(2)
      kafkaIO <- producer.produceAsync(record(topic), StringSerde, IntSerde)
      result <- kafkaIO.await
    } yield result === RecordMetadata(topic, partition = 1, offset = 0L)
  }

  "produce" in {
    for {
      TestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(2)
      result <- producer.produce(record(topic), StringSerde, IntSerde)
    } yield result === RecordMetadata(topic, partition = 1, offset = 0L)
  }

  "map to failure" in {
    for {
      _ <- Producer.makeR[Any](failFastInvalidBrokersConfig).use { producer =>
        for {
          failure <- producer.produce(record("no_such_topic"), StringSerde, IntSerde).flip
        } yield failure.getClass.getSimpleName === "TimeoutError"
      }
    } yield ok
  }

  private def failFastInvalidBrokersConfig =
    ProducerConfig("localhost:27461", ProducerRetryPolicy(0, 0.millis), Map("max.block.ms" -> "0"))

  private def record(topic: Topic) =
    ProducerRecord[String, Partition](topic, 100, partition = Some(1))
}