package com.wixpress.dst.greyhound.core.retry

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer.ConsumerConfigFailedValidation.InvalidRetryConfigForPatternSubscription
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.NonBlockingRetryHelper.fixedRetryTopic
import com.wixpress.dst.greyhound.core.consumer.retry._
import com.wixpress.dst.greyhound.core.producer.{Encryptor, ProducerRecord}
import com.wixpress.dst.greyhound.core.testkit.{eventuallyZ, AwaitableRef, BaseTestWithSharedEnv}
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig, TopicPartition}
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testenv.ITEnv._
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import zio.Clock.ClockLive
import zio._

import java.util.regex.Pattern
import zio.Clock

class RetryIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env = ITEnv.ManagedEnv

  override def sharedEnv = testResources()

  "configure a non-blocking retry policy" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic()
        anotherTopic                  <- kafka.createRandomTopic()
        group                         <- randomGroup

        invocations <- Ref.make(0)
        done        <- Promise.make[Nothing, ConsumerRecord[String, String]]
        retryConfig  = ZRetryConfig
                         .perTopicRetries {
                           case `topic`        => RetryConfigForTopic(() => Nil, NonBlockingBackoffPolicy(1.second :: Nil))
                           case `anotherTopic` => RetryConfigForTopic(() => Nil, NonBlockingBackoffPolicy(1.second :: Nil))
                         }
                         .copy(produceEncryptor = _ => ZIO.succeed(dummyEncryptor))

        toProduce    = ProducerRecord(topic, "bar", Some("foo"))
        retryHandler = failingRecordHandler(invocations, done).withDeserializers(StringSerde, StringSerde)
        successful  <- RecordConsumer.make(configFor(kafka, group, retryConfig, topic, anotherTopic), retryHandler).flatMap { _ =>
                         producer.produce(toProduce, StringSerde, StringSerde) *>
                           producer.produce(toProduce.copy(topic = anotherTopic), StringSerde, StringSerde) *> done.await.timeout(20.seconds)
                       }
      } yield successful must
        beSome(like[ConsumerRecord[String, String]] {
          case record =>
            (record.value === toProduce.value.get) and
              (record.key === toProduce.key) and
              (record.headers.headers.get(EncryptedHeader) must beSome)
        })
    }.withClock(ClockLive)

  "configure a handler with blocking retry policy" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic()
        topic2                        <- kafka.createRandomTopic()
        group                         <- randomGroup

        consumedValuesRef <- Ref.make(List.empty[String])

        retryConfig     =
          ZRetryConfig
            .finiteBlockingRetry(100.millis, 100.millis)
            .withCustomRetriesFor { case `topic2` => RetryConfigForTopic(() => 300.millis :: Nil, NonBlockingBackoffPolicy.empty) }
        retryHandler    = failingBlockingRecordHandlerWith(consumedValuesRef, Set(topic, topic2)).withDeserializers(StringSerde, StringSerde)
        _              <- RecordConsumer.make(configFor(kafka, group, retryConfig, topic, topic2), retryHandler).flatMap { _ =>
                            producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *> Clock.sleep(2.seconds) *>
                              producer.produce(ProducerRecord(topic2, "baz", Some("foo")), StringSerde, StringSerde) *> Clock.sleep(2.seconds)

                          }
        consumedValues <- consumedValuesRef.get
      } yield consumedValues === List("bar", "bar", "bar", "baz", "baz")
    }

  "configure a handler with blocking retry with exponential backoff policy" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic()
        group                         <- randomGroup

        consumedValuesRef <- Ref.make(List.empty[String])

        retryConfig     = ZRetryConfig.exponentialBackoffBlockingRetry(100.millis, 2, 1, infiniteRetryMaxInterval = false)
        retryHandler    = failingBlockingRecordHandler(consumedValuesRef, topic).withDeserializers(StringSerde, StringSerde)
        _              <- RecordConsumer.make(configFor(kafka, group, retryConfig, topic), retryHandler).flatMap { _ =>
                            producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *>
                              producer.produce(ProducerRecord(topic, "baz", Some("foo")), StringSerde, StringSerde) *> Clock.sleep(2.seconds)
                          }
        consumedValues <- consumedValuesRef.get
      } yield consumedValues.take(4) === Seq("bar", "bar", "bar", "baz")
    }.withClock(ClockLive)

  "configure a handler with infinite blocking retry policy" in
    {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic()
        group                         <- randomGroup

        consumedValuesRef <- Ref.make(List.empty[String])

        retryConfig     = ZRetryConfig.infiniteBlockingRetry(1.second)
        retryHandler    = failingBlockingRecordHandler(consumedValuesRef, topic, stopFailingAfter = 6)
                            .withDeserializers(StringSerde, StringSerde)
        _              <- ZIO.scoped(RecordConsumer.make(configFor(kafka, group, retryConfig, topic), retryHandler).flatMap { _ =>
                            producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *>
                              producer.produce(ProducerRecord(topic, "baz", Some("foo")), StringSerde, StringSerde) *> Clock.sleep(5.seconds)
                          })
        consumedValues <- consumedValuesRef.get
      } yield consumedValues must atLeast("bar", "bar", "bar", "bar", "bar")
    }.withClock(ClockLive)

  "block only one partition when GreyhoundBlockingException is thrown" in
    ZIO.scoped {
      for {
        r <- getShared
        TestResources(kafka, producer) = r
        topic <- kafka.createRandomTopic(2)
        group <- randomGroup
        firstMessageCallCount <- Ref.make[Int](0)
        secondPartitionCallCount <- Ref.make[Int](0)
        messageIntro = "message number "
        firstMessageContent = messageIntro + "1"
        secondPartitonContent = "second partition"
        numOfMessages = 10
        handler = failOnceOnOnePartitionHandler(firstMessageContent, secondPartitonContent, firstMessageCallCount, secondPartitionCallCount)
          .withDeserializers(StringSerde, StringSerde)
        retryConfig = ZRetryConfig.finiteBlockingRetry(3.second, 3.seconds, 3.seconds)
        consumer <- RecordConsumer.make(configFor(kafka, group, retryConfig, topic), handler)
        _ <- ZIO
          .foreach(1 to numOfMessages) { i =>
            producer.produce(ProducerRecord(topic, messageIntro + i, partition = Some(0)), StringSerde, StringSerde) *>
              producer.produce(ProducerRecord(topic, secondPartitonContent, partition = Some(1)), StringSerde, StringSerde)
          }
          .fork
      } yield {
        eventually {
          runsafe(firstMessageCallCount.get) === 1
          runsafe(secondPartitionCallCount.get) === numOfMessages
          runsafe(consumer.state).eventLoopState.dispatcherState.totalQueuedTasksPerTopic(topic) === (numOfMessages - 1)
        }
        eventually(10, 1.second.asScala) {
          runsafe(firstMessageCallCount.get) === 2
          runsafe(consumer.state).eventLoopState.dispatcherState.totalQueuedTasksPerTopic(topic) === 0
        }
      }
    }.withClock(ClockLive)

  "resume specific partition after blocking exception thrown" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(1)
        group                         <- randomGroup
        callCount                     <- Ref.make[Int](0)
        exceptionMessage               = "message that will throw exception"
        numOfMessages                  = 10

        handler     = blockResumeHandler(exceptionMessage, callCount)
                        .withDeserializers(StringSerde, StringSerde)
        retryConfig = ZRetryConfig.finiteBlockingRetry(30.seconds, 30.seconds)

        consumer <- RecordConsumer.make(configFor(kafka, group, retryConfig, topic), handler)
        _        <- producer.produce(ProducerRecord(topic, exceptionMessage), StringSerde, StringSerde)
        _        <- ZIO.foreach(1 to numOfMessages) { _ => producer.produce(ProducerRecord(topic, "irrelevant"), StringSerde, StringSerde) }.fork
        _        <- eventuallyZ(callCount.get)(_ == 1)
        _        <- consumer.setBlockingState(IgnoreOnceFor(TopicPartition(topic, 0)))
        _        <- eventuallyZ(callCount.get)(_ == (numOfMessages + 1))
      } yield ok
    }.withClock(ClockLive)

  "commit message on failure with NonRetryableException" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(partitions = 1) // sequential processing is needed
        group                         <- randomGroup
        invocations                   <- Ref.make(0)
        retryConfig                    = ZRetryConfig.nonBlockingRetry(100.milliseconds, 100.milliseconds, 100.milliseconds)
        retryHandler                   = failingNonRetryableRecordHandler(invocations).withDeserializers(StringSerde, StringSerde)
        invocations                   <- RecordConsumer.make(configFor(kafka, group, retryConfig, topic), retryHandler).flatMap { _ =>
                                           producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *>
                                             invocations.get.delay(2.seconds)
                                         }
      } yield invocations mustEqual 1
    }.withClock(ClockLive)

  "configure a regex consumer with a retry policy" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        _                             <- kafka.createTopic(TopicConfig("topic-111", 1, 1, CleanupPolicy.Delete(1.hour.toMillis)))
        group                         <- randomGroup
        invocations                   <- Ref.make(0)
        done                          <- Promise.make[Nothing, ConsumerRecord[String, String]]
        retryConfig                    = ZRetryConfig.retryForPattern(
                                           RetryConfigForTopic(() => Nil, NonBlockingBackoffPolicy(Seq(1.second, 1.second, 1.seconds)))
                                         )
        retryHandler                   = failingRecordHandler(invocations, done).withDeserializers(StringSerde, StringSerde)
        success                       <- RecordConsumer
                                           .make(
                                             RecordConsumerConfig(
                                               kafka.bootstrapServers,
                                               group,
                                               initialSubscription = TopicPattern(Pattern.compile("topic-1.*")),
                                               retryConfig = Some(retryConfig),
                                               extraProperties = fastConsumerMetadataFetching,
                                               offsetReset = OffsetReset.Earliest
                                             ),
                                             retryHandler
                                           )
                                           .flatMap { _ =>
                                             producer.produce(ProducerRecord("topic-111", "bar", Some("foo")), StringSerde, StringSerde) *>
                                               done.await.timeout(20.seconds)
                                           }
      } yield success must beSome
    }.withClock(ClockLive)

  "configure a handler with blocking followed by non blocking retry policy" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic()
        group                         <- randomGroup
        originalTopicCallCount        <- Ref.make[Int](0)
        retryTopicCallCount           <- Ref.make[Int](0)
        retryConfig                    = ZRetryConfig.blockingFollowedByNonBlockingRetry(List(1.second), NonBlockingBackoffPolicy(List(1.seconds)))
        retryHandler                   = failingBlockingNonBlockingRecordHandler(originalTopicCallCount, retryTopicCallCount, topic).withDeserializers(
                                           StringSerde,
                                           StringSerde
                                         )
        _                             <- RecordConsumer.make(configFor(kafka, group, retryConfig, topic), retryHandler).flatMap { _ =>
                                           producer.produce(ProducerRecord(topic, "bar", Some("foo")), StringSerde, StringSerde) *> Clock.sleep(5.seconds)
                                         }
      } yield {
        eventually {
          runsafe(originalTopicCallCount.get) === 2
          runsafe(retryTopicCallCount.get) === 1
        }
      }
    }.withClock(ClockLive)

  "fail validation for mismatching pattern retry policy with regular subscription" in
    ZIO.scoped {
      for {
        r                      <- getShared
        TestResources(kafka, _) = r
        retryConfig             = ZRetryConfig.retryForPattern(
                                    RetryConfigForTopic(() => Nil, NonBlockingBackoffPolicy(Seq(1.second, 1.second, 1.seconds)))
                                  )
        handler                 = RecordHandler { _: ConsumerRecord[String, String] => ZIO.unit }.withDeserializers(StringSerde, StringSerde)
        _                      <- RecordConsumer
                                    .make(configFor(kafka, "group", retryConfig, "topic"), handler)
                                    .flip
                                    .flatMap(t => ZIO.succeed(t === InvalidRetryConfigForPatternSubscription))
      } yield ok
    }.withClock(ClockLive)

  "release consumer during non-blocking retry back-off" in
    ZIO.scoped {
      val drainTimeout = 5.seconds
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(1)
        group                         <- randomGroup
        attempt                       <- AwaitableRef.make(0)
        handler                        = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                                           attempt.update(_ + 1) *> ZIO.fail(new RuntimeException("boom!"))
                                         }
        retryConfig                    = ZRetryConfig.nonBlockingRetry(15.seconds)
        payload                       <- randomId
        _                             <-
          ZIO.scoped(
            RecordConsumer.make(configFor(kafka, group, retryConfig, topic).withDrainTimeout(drainTimeout), handler) *>
              producer.produce(ProducerRecord(topic, payload, Some("someKey")), StringSerde, StringSerde) *> attempt.await(_ >= 1) *>
              Clock.sleep(3.seconds)
          )
        // now we start a new consumer on the retry topic directly and expect to consume the message
        consumedPayloads              <- AwaitableRef.make(Seq.empty[String])
        collectingHandler              = RecordHandler { rec: ConsumerRecord[String, String] => consumedPayloads.update(_ :+ rec.value) }
                                           .withDeserializers(StringSerde, StringSerde)
        retryTopic                     = fixedRetryTopic(topic, group, 0)
        consumed                      <- RecordConsumer.make(consumerConfig(kafka, group, retryTopic, OffsetReset.Latest), collectingHandler).flatMap { _ =>
                                           consumedPayloads.await(_.nonEmpty)
                                         }
      } yield {
        consumed === Seq(payload)
      }
    }.withClock(ClockLive)

  private def configFor(kafka: ManagedKafka, group: String, retryConfig: RetryConfig, topics: String*) = {
    RecordConsumerConfig(
      kafka.bootstrapServers,
      group,
      initialSubscription = Topics(topics.toSet),
      retryConfig = Some(retryConfig),
      extraProperties = fastConsumerMetadataFetching,
      offsetReset = OffsetReset.Earliest
    )
  }

  private def consumerConfig(kafka: ManagedKafka, group: String, topic: String, offsetReset: OffsetReset = OffsetReset.Earliest) = {
    RecordConsumerConfig(
      kafka.bootstrapServers,
      group,
      initialSubscription = Topics(Set(topic)),
      extraProperties = fastConsumerMetadataFetching,
      offsetReset = offsetReset
    )
  }

  private implicit class RecordConsumerConfigOps(val config: RecordConsumerConfig) {
    def withDrainTimeout(duration: Duration) = config.copy(eventLoopConfig = config.eventLoopConfig.copy(drainTimeout = duration))
  }

  private def blockResumeHandler(exceptionMessage: String, callCount: Ref[Int]) = {
    RecordHandler { r: ConsumerRecord[String, String] =>
      callCount.get.flatMap(count => ZIO.succeed(println(s">>>> in handler: r: $r callCount before: $count"))) *> callCount.update(_ + 1) *>
        ZIO.when(r.value == exceptionMessage) {
          ZIO.fail(SomeException())
        }
    }
  }

  private def failOnceOnOnePartitionHandler(
    firstMessageContent: String,
    secondPartitonContent: String,
    firstMessageCallCount: Ref[Int],
    secondPartitionCallCount: Ref[Int]
  ) = {
    RecordHandler { r: ConsumerRecord[String, String] =>
      ZIO.when(r.value == firstMessageContent) {
        firstMessageCallCount
          .updateAndGet(_ + 1)
          .flatMap(count => {
            if (count == 1)
              ZIO.fail(SomeException())
            else
              ZIO.unit
          })
      } *>
        ZIO.when(r.value == secondPartitonContent) {
          secondPartitionCallCount.update(_ + 1)
        }
    }
  }

  def failingBlockingNonBlockingRecordHandler(originalTopicCallCount: Ref[Int], retryTopicCallCount: Ref[Int], topic: String) = {
    RecordHandler { r: ConsumerRecord[String, String] =>
      (if (r.topic == topic)
         originalTopicCallCount.update(_ + 1)
       else
         retryTopicCallCount.update(_ + 1)) *> ZIO.fail(SomeException())
    }
  }

  private def failingRecordHandler(invocations: Ref[Int], done: Promise[Nothing, ConsumerRecord[String, String]]) =
    RecordHandler { r: ConsumerRecord[String, String] =>
      invocations.updateAndGet(_ + 1).flatMap { n =>
        if (n < 4) {
          println(s"failling.. $n")
          ZIO.fail(new RuntimeException("Oops!"))
        } else {
          println(s"success!  $n")
          done.succeed(r) // Succeed on final retry
        }
      }
    }

  private def failingNonRetryableRecordHandler(originalTopicInvocations: Ref[Int]) =
    RecordHandler { _: ConsumerRecord[String, String] =>
      originalTopicInvocations.updateAndGet(_ + 1) *> ZIO.fail(NonRetriableException(new RuntimeException("Oops!")))
    }

  private def failingBlockingRecordHandler[R](consumedValues: Ref[List[String]], originalTopic: String, stopFailingAfter: Int = 5000) =
    failingBlockingRecordHandlerWith(consumedValues, Set(originalTopic), stopFailingAfter)

  private def failingBlockingRecordHandlerWith[R](
    consumedValues: Ref[List[String]],
    originalTopics: Set[String],
    stopFailingAfter: Int = 5000
  ) =
    RecordHandler { r: ConsumerRecord[String, String] =>
      ZIO.succeed(println(s">>>> failingBlockingRecordHandler: r ${r}")) *>
        ZIO.when(originalTopics.contains(r.topic)) {
          consumedValues.updateAndGet(values => values :+ r.value)
        } *>
        consumedValues.get.flatMap { values =>
          if (values.size <= stopFailingAfter)
            ZIO.fail(new RuntimeException("Oops!"))
          else
            ZIO.unit
        }

    }

  private def fastConsumerMetadataFetching = Map("metadata.max.age.ms" -> "0")

  private lazy val EncryptedHeader = "Encrypted"
  private lazy val dummyEncryptor  = new Encryptor {
    override def encrypt[K](record: ProducerRecord[K, Chunk[Byte]])(implicit trace: Trace): Task[ProducerRecord[K, Chunk[Byte]]] =
      ZIO.succeed(record.copy(headers = record.headers + (EncryptedHeader, Chunk.fromArray("true".getBytes))))
  }

  private def runsafe[E, A](f: ZIO[Any, E, A]) =
    zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(f).getOrThrowFiberFailure() }
}

case class SomeException() extends Exception("expected!", null, true, false)
