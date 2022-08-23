package com.wixpress.dst.greyhound.core.offset

import java.util.concurrent.TimeUnit

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.{OffsetReset, RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.BaseTestWithSharedEnv
import com.wixpress.dst.greyhound.core.zioutils.CountDownLatch
import com.wixpress.dst.greyhound.core.{Group, Topic, TopicPartition}
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testenv.ITEnv._
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import zio._

class OffsetIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env: UManaged[Env] = ITEnv.ManagedEnv

  override def sharedEnv: ZManaged[Env, Throwable, TestResources] = testResources()

  "return offset for time" in {
    for {
      TestResources(kafka, producer) <- getShared
      topic                          <- kafka.createRandomTopic(prefix = s"aTopic", partitions = 1)
      partition                       = 0
      group                          <- randomGroup

      numberOfMessages     = 32
      someMessages         = 16
      handledAllMessages  <- CountDownLatch.make(numberOfMessages)
      handledSomeMessages <- CountDownLatch.make(someMessages)
      handler              = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                               handledSomeMessages.countDown zipParRight handledAllMessages.countDown
                             }

      test <- RecordConsumer.make(configFor(topic, group, kafka).copy(offsetReset = OffsetReset.Earliest), handler).use { consumer =>
                val record = ProducerRecord(topic, Chunk.empty)
                for {
                  zeroTime         <- clock.currentTime(TimeUnit.MILLISECONDS)
                  _                <- ZIO.foreachPar_(0 until someMessages)(_ => producer.produce(record))
                  _                <- handledSomeMessages.await
                  middleTime       <- clock.currentTime(TimeUnit.MILLISECONDS)
                  // When we don't produce the additional messages below the test starts failing as Kafka client returns null for
                  // middleTimeOffset - WTF?
                  _                <- ZIO.foreachPar_(someMessages until numberOfMessages)(_ => producer.produce(record))
                  _                <- handledAllMessages.await
                  zeroTimeOffset   <- consumer.offsetsForTimes(Map(TopicPartition(topic, partition) -> zeroTime))
                  middleTimeOffset <- consumer.offsetsForTimes(Map(TopicPartition(topic, partition) -> middleTime))
                } yield {
                  (zeroTimeOffset(TopicPartition(topic, partition)) === 0) and
                    (middleTimeOffset(TopicPartition(topic, partition)) === someMessages)
                }
              }
    } yield test
  }

  private def configFor(topic: Topic, group: Group, kafka: ManagedKafka) =
    RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic)))

}
