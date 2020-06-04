package com.wixpress.dst.greyhound.core.retry

import java.util.regex.Pattern

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.core.consumer.ConsumerSubscription.{TopicPattern, Topics}
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

        invocations <- Ref.make(0)
        done <- Promise.make[Nothing, Unit]
        retryPolicy = RetryPolicy.default(group, 1.second, 1.seconds, 1.seconds)
        retryHandler = failingRecordHandler(invocations, done).withDeserializers(StringSerde, StringSerde)
        success <- RecordConsumer.make(RecordConsumerConfig(kafka.bootstrapServers, group,
          initialSubscription = Topics(Set(topic)), retryPolicy = Some(retryPolicy)), retryHandler).use_ {
          producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *>
            done.await.timeout(20.seconds)
        }
      } yield success must beSome
    }

  "configure a regex consumer with a retry policy" in {
    for {
      TestResources(kafka, producer) <- getShared
      _ <- kafka.createTopic(TopicConfig("topic-111", 1, 1, CleanupPolicy.Delete(1.hour.toMillis)))
      group <- randomGroup
      invocations <- Ref.make(0)
      done <- Promise.make[Nothing, Unit]
      retryPolicy = RetryPolicy.default(group, 1.second, 1.seconds, 1.seconds)
      retryHandler = failingRecordHandler(invocations, done).withDeserializers(StringSerde, StringSerde)
      success <- RecordConsumer.make(RecordConsumerConfig(kafka.bootstrapServers, group,
        initialSubscription = TopicPattern(Pattern.compile("topic-1.*")), retryPolicy = Some(retryPolicy),
        extraProperties = fastConsumerMetadataFetching), retryHandler).use_ {
        producer.produce(ProducerRecord("topic-111", "bar", Some("foo")), StringSerde, StringSerde) *>
          done.await.timeout(20.seconds)
      }
    } yield success must beSome
  }


  private def failingRecordHandler(invocations: Ref[Int], done: Promise[Nothing, Unit]) =
    RecordHandler { _: ConsumerRecord[String, String] =>
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

  private def fastConsumerMetadataFetching = Map("metadata.max.age.ms" -> "0")
}

