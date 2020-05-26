package com.wixpress.dst.greyhound.core.retry

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.TopicConfig
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.BaseTestWithSharedEnv
import com.wixpress.dst.greyhound.testkit.ITEnv
import com.wixpress.dst.greyhound.testkit.ITEnv._
import zio._
import zio.duration._

class RetryIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env: UManaged[Env] = ITEnv.ManagedEnv

  override def sharedEnv: ZManaged[Env, Throwable, TestResources] = testResources()

  "configure a handler with retry policy" in {
    for {
      TestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic()
      group <- randomGroup
      _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-0", partitions, 1, delete))
      _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-1", partitions, 1, delete))
      _ <- kafka.createTopic(TopicConfig(s"$topic-$group-retry-2", partitions, 1, delete))

      invocations <- Ref.make(0)
      done <- Promise.make[Nothing, Unit]
      retryPolicy = RetryPolicy.default(group, 1.second, 1.seconds, 1.seconds)
      handler = RecordHandler { _: ConsumerRecord[String, String] =>
        invocations.updateAndGet(_ + 1).flatMap { n =>
          if (n < 4) {
            println(s"failling.. $n")
            ZIO.fail(new RuntimeException("Oops!"))
          } else {
            println(s"success!  $n")
            done.succeed(()) // Succeed on final retry
          }
        }
      }
      retryHandler = handler
        .withDeserializers(StringSerde, StringSerde)

      success <- RecordConsumer.make(RecordConsumerConfig(kafka.bootstrapServers, group,
        initialTopics = Set(topic), retryPolicy = Some(retryPolicy)), retryHandler).use_ {
        producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *>
          done.await.timeout(20.seconds)
      }
    } yield {
      success must beSome
    }
  }
}

