package com.wixpress.dst.greyhound.core

import java.util.concurrent.{TimeUnit, TimeoutException}
import java.util.regex.Pattern
import java.util.regex.Pattern.compile
import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.PollingFailed
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.OffsetReset.{Earliest, Latest}
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, Decryptor, RecordHandler}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.core.testkit.{AwaitableRef, BaseTestWithSharedEnv, TestMetrics, eventuallyTimeoutFail, eventuallyZ}
import com.wixpress.dst.greyhound.core.zioutils.CountDownLatch
import com.wixpress.dst.greyhound.core.zioutils.Gate
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testenv.ITEnv.{clientId, _}
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import zio.Clock
import zio.stm.{STM, TRef}
import zio._
import zio.{Clock, Console, _}
import zio.Clock.sleep
import zio.managed._

class ConsumerIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env = ITEnv.ManagedEnv

  override def sharedEnv = ITEnv.testResources()

  val resources = testResources()
  "produce, consume and rebalance - " in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(prefix = s"topic1-single1")
      topic2                        <- kafka.createRandomTopic(prefix = "topic2-single1")
      group                         <- randomGroup

      queue  <- Queue.unbounded[ConsumerRecord[String, String]]
      handler = RecordHandler((cr: ConsumerRecord[String, String]) => ZIO.succeed(println(s"***** Consumed: $cr")) *> queue.offer(cr))
        .withDeserializers(StringSerde, StringSerde)
        .ignore
      cId    <- clientId
      config  = configFor(kafka, group, topic).copy(clientId = cId)
      record  = ProducerRecord(topic, "bar", Some("foo"))

      messages <- RecordConsumer.make(config, handler).flatMap { consumer =>
        producer.produce(record, StringSerde, StringSerde) *> sleep(3.seconds) *>
          consumer.resubscribe(ConsumerSubscription.topics(topic, topic2)) *>
          sleep(500.millis) *> // give the consumer some time to start polling topic2
          producer.produce(record.copy(topic = topic2, value = Some("BAR")), StringSerde, StringSerde) *>
          (queue.take zip queue.take)
            .timeout(20.seconds)
            .tap(o => ZIO.when(o.isEmpty)(ZIO.debug("timeout waiting for messages!")))
      }
      msgs     <- ZIO.fromOption(messages).orElseFail(TimedOutWaitingForMessages)
    } yield {
      msgs._1 must
        (beRecordWithKey("foo") and beRecordWithValue("bar")) and
        (msgs._2 must (beRecordWithKey("foo") and beRecordWithValue("BAR")))
    }
  }

  "be able to resubscribe to same topics" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(prefix = s"topic1-single1")
      group                         <- randomGroup

      queue  <- Queue.unbounded[ConsumerRecord[String, String]]
      handler = RecordHandler((cr: ConsumerRecord[String, String]) => ZIO.succeed(println(s"***** Consumed: $cr")) *> queue.offer(cr))
        .withDeserializers(StringSerde, StringSerde)
        .ignore
      cId    <- clientId
      config  = configFor(kafka, group, topic, mutateEventLoop = _.copy(drainTimeout = 1.second)).copy(clientId = cId)
      record  = ProducerRecord(topic, "bar", Some("foo"))

      messages <- RecordConsumer.make(config, handler).flatMap { consumer =>
        producer.produce(record, StringSerde, StringSerde) *> sleep(3.seconds) *>
          consumer.resubscribe(ConsumerSubscription.topics(topic)) *>
          consumer.resubscribe(ConsumerSubscription.topics(topic)) *>
          producer.produce(record.copy(topic = topic, value = Some("BAR")), StringSerde, StringSerde) *>
          (queue.take zip queue.take)
            .timeout(20.seconds)
            .tap(o => ZIO.when(o.isEmpty)(ZIO.debug("timeout waiting for messages!")))
      }
      msgs     <- ZIO.fromOption(messages).orElseFail(TimedOutWaitingForMessages)
    } yield {
      msgs._1 must
        (beRecordWithKey("foo") and beRecordWithValue("bar")) and
        (msgs._2 must (beRecordWithKey("foo") and beRecordWithValue("BAR")))
    }
  }

  "produce consume null values (tombstones)" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(prefix = s"topic1-single1")
      group                         <- randomGroup

      queue   <- Queue.unbounded[ConsumerRecord[String, String]]
      handler  = RecordHandler((cr: ConsumerRecord[String, String]) => ZIO.succeed(println(s"***** Consumed: $cr")) *> queue.offer(cr))
        .withDeserializers(StringSerde, StringSerde)
        .ignore
      cId     <- clientId
      config   = configFor(kafka, group, topic).copy(clientId = cId)
      record   = ProducerRecord.tombstone(topic, Some("foo"))
      message <- RecordConsumer.make(config, handler).flatMap { _ =>
        producer.produce(record, StringSerde, StringSerde) *> queue.take.timeoutFail(TimedOutWaitingForMessages)(10.seconds)
      }
    } yield {
      message must (beRecordWithKey("foo") and beRecordWithValue(null))
    }
  }

  "not lose any messages on a slow consumer (drives the message dispatcher to throttling)" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      _                             <- ZIO.debug(">>>> starting test: throttlingTest")

      topic <- kafka.createRandomTopic(partitions = 2, prefix = "core-not-lose")
      group <- randomGroup

      messagesPerPartition = 500 // Exceeds the queue capacity
      delayPartition1     <- Promise.make[Nothing, Unit]
      handledPartition0   <- CountDownLatch.make(messagesPerPartition)
      handledPartition1   <- CountDownLatch.make(messagesPerPartition)
      handler              = RecordHandler { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        record.partition match {
          case 0 => handledPartition0.countDown
          case 1 => delayPartition1.await *> handledPartition1.countDown
        }
      }

      test <- RecordConsumer.make(configFor(kafka, group, topic), handler).flatMap { _ =>
        val recordPartition0 = ProducerRecord(topic, Chunk.empty, partition = Some(0))
        val recordPartition1 = ProducerRecord(topic, Chunk.empty, partition = Some(1))
        for {
          _                        <- ZIO.foreachParDiscard(0 until messagesPerPartition) { _ =>
            producer.produce(recordPartition0) zipPar producer.produce(recordPartition1)
          }
          handledAllFromPartition0 <- handledPartition0.await.timeout(10.seconds)
          _                        <- delayPartition1.succeed(())
          handledAllFromPartition1 <- handledPartition1.await.timeout(10.seconds)
        } yield {
          (handledAllFromPartition0 must beSome) and (handledAllFromPartition1 must beSome)
        }
      }
    } yield test
  }

  "delay resuming a paused partition" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      _                             <- ZIO.debug(">>>> starting test: delay resuming a paused partition")

      topic <- kafka.createRandomTopic(partitions = 1, prefix = "core-not-lose")
      group <- randomGroup

      messagesPerPartition = 500 // Exceeds the queue capacity
      delayPartition      <- Promise.make[Nothing, Unit]
      handledPartition    <- CountDownLatch.make(messagesPerPartition)
      handler              = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => delayPartition.await *> handledPartition.countDown }
      start               <- Clock.currentTime(TimeUnit.MILLISECONDS)
      test                <- RecordConsumer
        .make(configFor(kafka, group, topic, mutateEventLoop = _.copy(delayResumeOfPausedPartition = 3000)), handler)
        .flatMap { _ =>
          val recordPartition = ProducerRecord(topic, Chunk.empty, partition = Some(0))
          for {
            _                       <- ZIO.foreachParDiscard(0 until messagesPerPartition) { _ => producer.produce(recordPartition) }
            _                       <- delayPartition.succeed(()).delay(1.seconds).fork
            handledAllFromPartition <- handledPartition.await.timeout(10.seconds)
            end                     <- Clock.currentTime(TimeUnit.MILLISECONDS)

          } yield {
            (handledAllFromPartition aka "handledAllFromPartition" must beSome) and
              (end - start aka "complete handling duration" must beGreaterThan(3000L))
          }
        }
    } yield test
  }

  "pause and resume consumer" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      _                             <- ZIO.debug(">>>> starting test: pauseResumeTest")

      topic <- kafka.createRandomTopic(prefix = "core-pause-resume")
      group <- randomGroup

      numberOfMessages     = 32
      someMessages         = 16
      restOfMessages       = numberOfMessages - someMessages
      handledSomeMessages <- CountDownLatch.make(someMessages)
      handledAllMessages  <- CountDownLatch.make(numberOfMessages)
      handleCounter       <- Ref.make[Int](0)
      handler              = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        handleCounter.update(_ + 1) *> handledSomeMessages.countDown zipParRight handledAllMessages.countDown
      }

      test <- RecordConsumer.make(configFor(kafka, group, topic).copy(offsetReset = OffsetReset.Earliest), handler).flatMap { consumer =>
        val record = ProducerRecord(topic, Chunk.empty)
        for {
          _                 <- ZIO.foreachParDiscard(0 until someMessages)(_ => producer.produce(record))
          _                 <- handledSomeMessages.await
          _                 <- consumer.pause
          _                 <- ZIO.foreachParDiscard(0 until restOfMessages)(_ => producer.produce(record))
          a                 <- handledAllMessages.await.timeout(5.seconds)
          handledAfterPause <- handleCounter.get
          _                 <- consumer.resume
          b                 <- handledAllMessages.await.timeout(5.seconds)
        } yield {
          (handledAfterPause === someMessages) and (a must beNone) and (b must beSome)
        }
      }
    } yield test
  }

  "wait until queues are drained" in {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      _                             <- ZIO.debug(">>>> starting test: gracefulShutdownTest")
      topic                         <- kafka.createRandomTopic(prefix = "core-wait-until")
      group                         <- randomGroup

      ref                    <- Ref.make(0)
      startedHandling        <- Promise.make[Nothing, Unit]
      handler: Handler[Any] = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        startedHandling.succeed(()) *> sleep(5.seconds) *> ref.update(_ + 1)
      }

      _ <- ZIO.scoped(RecordConsumer.make(configFor(kafka, group, topic), handler).flatMap { _ =>
        producer.produce(ProducerRecord(topic, Chunk.empty)) *> startedHandling.await
      })

      handled <- ref.get
    } yield {
      handled must equalTo(1)
    }
  }

  "consumer from earliest offset" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      _                             <- ZIO.debug(">>>> starting test: earliestTest")
      topic                         <- kafka.createRandomTopic(prefix = "core-from-earliest")
      group                         <- randomGroup

      queue  <- Queue.unbounded[ConsumerRecord[String, String]]
      handler = RecordHandler(queue.offer(_: ConsumerRecord[String, String]))
        .withDeserializers(StringSerde, StringSerde)
        .ignore

      record = ProducerRecord(topic, "bar", Some("foo"))
      _     <- producer.produce(record, StringSerde, StringSerde)

      message <- RecordConsumer
        .make(configFor(kafka, group, topic).copy(offsetReset = Earliest), handler)
        .flatMap { _ => queue.take }
        .timeout(10.seconds)
    } yield {
      message.get must (beRecordWithKey("foo") and beRecordWithValue("bar"))
    }
  }

  "not lose messages while throttling after rebalance" in ZIO.scoped {
    for {
      _                             <- ZIO.debug(">>>> starting test: throttleWhileRebalancingTest")
      r                             <- getShared
      TestResources(kafka, producer) = r
      partitions                     = 30
      topic                         <- kafka.createRandomTopic(partitions, prefix = "core-not-lose-while-throttling")
      group                         <- randomGroup
      probe                         <- Ref.make(Map.empty[Partition, Seq[Offset]])
      messagesPerPartition           = 250
      handler                        = RecordHandler { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        sleep(10.millis) *>
          probe
            .getAndUpdate(curr => curr + (record.partition -> (curr.getOrElse(record.partition, Nil) :+ record.offset)))
            .flatMap(map =>
              ZIO.when(map.getOrElse(record.partition, Nil).contains(record.offset))(
                ZIO.debug(OffsetWasAlreadyProcessed(record.partition, record.offset).toString)
              )
            )
      }
      createConsumerTask             = (i: Int) => makeConsumer(kafka, topic, group, handler, i)
      test                          <- createConsumerTask(0).flatMap { _ =>
        val record = ProducerRecord(topic, Chunk.empty, partition = Some(0))
        for {
          env <- ZIO.environment[Env]
          _           <- ZIO.foreachParDiscard(0 until partitions) { p =>
            ZIO.foreachDiscard(0 until messagesPerPartition)(_ => producer.produceAsync(record.copy(partition = Some(p))))
          }
          _           <- createConsumerTask(1).provideEnvironment(env.add(Scope.global)).forkScoped // rebalance
          _           <- createConsumerTask(2).provideEnvironment(env.add(Scope.global)).forkScoped // rebalance // rebalance
          expected     = (0 until partitions).map(p => (p, 0L until messagesPerPartition)).toMap
          _           <- eventuallyTimeoutFail(probe.get)(m =>
            m.mapValues(_.lastOption).values.toSet == Set(Option(messagesPerPartition - 1L)) && m.size == partitions
          )(120.seconds)
          finalResult <- probe.get
          _           <- ZIO.debug(finalResult.mapValues(_.size).mkString(","))
        } yield finalResult === expected
      }
    } yield test
  }

  "subscribe to a pattern" in ZIO.scoped {
    for {
      _                             <- ZIO.debug(">>>> starting test: patternTest")
      topic1                         = "core-subscribe-pattern1-topic"
      topic2                         = "core-subscribe-pattern2-topic"
      r                             <- getShared
      TestResources(kafka, producer) = r
      _                             <- kafka.createTopics(Seq(topic1, topic2).map(t => TopicConfig(t, 1, 1, delete)): _*)
      group                         <- randomGroup
      probe                         <- Ref.make(Seq.empty[(Topic, Offset)])
      handler                        = RecordHandler { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => probe.update(_ :+ (record.topic, record.offset)) }

      _ <- makeConsumer(kafka, compile("core-subscribe-pattern1.*"), group, handler, 0).flatMap { consumer =>
        val record = ProducerRecord(topic1, Chunk.empty, key = Option(Chunk.empty))

        producer.produce(record) *> eventuallyZ(probe.get)(_ == (topic1, 0) :: Nil) *>
          consumer.resubscribe(TopicPattern(compile("core-subscribe-pattern2.*"))) *> producer.produce(record.copy(topic = topic2)) *>
          eventuallyZ(probe.get)(_ == (topic1, 0) :: (topic2, 0) :: Nil)
      }
    } yield ok
  }

  "consumer from a new partition is interrupted before commit (offsetReset = Latest)" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(1)
      group                         <- randomGroup
      handlingStarted               <- Promise.make[Nothing, Unit]
      hangForever                   <- Promise.make[Nothing, Unit]
      hangingHandler                 = RecordHandler { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        handlingStarted.complete(ZIO.unit) *> hangForever.await
      }
      record                        <- aProducerRecord(topic)
      recordValue                    = record.value.get
      _                             <- makeConsumer(kafka, topic, group, hangingHandler, 0, _.copy(drainTimeout = 200.millis))
        .flatMap { consumer =>
          producer.produce(record, StringSerde, StringSerde) *> handlingStarted.await *>
            // unsubscribe to make sure partitions are released
            consumer.resubscribe(ConsumerSubscription.topics())
        }
        .disconnect
        .timeoutFail(new TimeoutException("timed out waiting for consumer 0"))(10.seconds)
      consumed                      <- AwaitableRef.make(Seq.empty[String])
      handler                        = RecordHandler { record: ConsumerRecord[String, String] => consumed.update(_ :+ record.value) }
        .withDeserializers(StringSerde, StringSerde)

      consumedValues <- makeConsumer(kafka, topic, group, handler, 1, modifyConfig = _.copy(offsetReset = Latest))
        .flatMap { _ => consumed.await(_.nonEmpty, 5.seconds) }
        .disconnect
        .timeoutFail(new TimeoutException("timed out waiting for consumer 1"))(10.seconds)
    } yield {
      consumedValues must contain(recordValue)
    }
  }

  "block until current tasks complete" in ZIO.scoped {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(prefix = "block-until")
      group                         <- randomGroup

      waitForTasksDuration <- TRef.make[Duration](0.millis).commit
      innerGate            <- Gate.make(initiallyAllow = false)
      outerGate            <- Gate.make(initiallyAllow = false)
      handler               = RecordHandler((_: ConsumerRecord[String, String]) => outerGate.toggle(true) *> innerGate.await())
        .withDeserializers(StringSerde, StringSerde)
        .ignore
      config                = configFor(kafka, group, topic)
      record                = ProducerRecord(topic, "bar", Some("foo"))

      test <- RecordConsumer.make(config, handler).flatMap { consumer =>
        for {
          _                  <- ZIO.foreach((0 until 100).toSet)(_ => producer.produce(record, StringSerde, StringSerde)) *>
            outerGate.await() /* handler waiting on innerGate now */ *>
            consumer.waitForCurrentRecordsCompletion.timed
              .map(_._1) /* after 'delay' the consumer's innerGate opens and handler completes */
              .flatMap(d => waitForTasksDuration.set(d).commit)
              .fork
          delay               = 1.second
          tasksDurationFiber <- waitForTasksDuration.get.tap(d => STM.check(d > 0.millis)).commit.fork
          _                  <- innerGate.toggle(true).delay(delay) /*and now handler will complete */
          tasksDuration      <- tasksDurationFiber.join
        } yield tasksDuration must between(delay * 0.9, delay * 3)
      }
    } yield test
  }

  "rewind positions on poll failure" in ZIO.scoped {
    type BinaryRecord    = ConsumerRecord[Chunk[Byte], Chunk[Byte]]
    type BinaryDecryptor = Decryptor[Any, RuntimeException, Chunk[Byte], Chunk[Byte]]
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(prefix = "poll-fail")
      group                         <- randomGroup
      failToDecrypt                 <- Ref.make(false)
      messages                      <- AwaitableRef.make[Seq[ConsumerRecord[String, String]]](Nil)
      handler                        = RecordHandler((cr: ConsumerRecord[String, String]) => ZIO.debug(s"***** Consumed: $cr") *> messages.update(_ :+ cr))
        .withDeserializers(StringSerde, StringSerde)
        .ignore
      cId                           <- clientId
      decryptor: BinaryDecryptor     = new BinaryDecryptor {
        override def decrypt(record: ConsumerRecord[Chunk[Byte], Chunk[Byte]])(implicit trace: Trace): ZIO[Any, RuntimeException, ConsumerRecord[Chunk[Byte], Chunk[Byte]]] =
          ZIO.whenZIO(failToDecrypt.get)(ZIO.fail(new RuntimeException)).map(_ => record)
      }

      pollFailedMetrics <- TestMetrics.queue

      config  = configFor(kafka, group, topic).copy(clientId = cId, decryptor = decryptor)
      aRecord = (i: Int) => ProducerRecord(topic, s"payload-$i", Some(s"key-$i"))
      _      <- RecordConsumer.make(config, handler).flatMap { consumer =>
        val Seq(rec1, rec2) = (1 to 2) map aRecord
        consumer.resubscribe(ConsumerSubscription.topics(topic)) *> producer.produce(rec1, StringSerde, StringSerde) *>
          messages.await(_.exists(_.value == rec1.value.get)) *> failToDecrypt.set(true) *>
          producer.produce(rec2, StringSerde, StringSerde) *> next[PollingFailed](pollFailedMetrics) *> failToDecrypt.set(false) *>
          messages.await(_.exists(_.value == rec2.value.get), 5.seconds)
      }
    } yield ok
  }


  def next[A <: GreyhoundMetric](queue: Queue[GreyhoundMetric]) =
    queue.take.repeatUntil(_.isInstanceOf[A])

  private def makeConsumer[E](
    kafka: ManagedKafka,
    topic: String,
    group: String,
    handler: RecordHandler[Any, E, Chunk[Byte], Chunk[Byte]],
    i: Int,
    mutateEventLoop: EventLoopConfig => EventLoopConfig = identity,
    modifyConfig: RecordConsumerConfig => RecordConsumerConfig = identity
  ): ZIO[RecordConsumer.Env with Scope, Throwable, RecordConsumer[RecordConsumer.Env]] =
    RecordConsumer.make(
      modifyConfig(configFor(kafka, group, topic, mutateEventLoop).copy(clientId = s"client-$i", offsetReset = OffsetReset.Earliest)),
      handler
    )

  private def makeConsumer(
    kafka: ManagedKafka,
    pattern: Pattern,
    group: String,
    handler: RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]],
    i: Int
  ) =
    RecordConsumer.make(configFor(kafka, group, pattern).copy(clientId = s"client-$i", offsetReset = OffsetReset.Earliest), handler)

  private def configFor(kafka: ManagedKafka, group: Group, topic: Topic, mutateEventLoop: EventLoopConfig => EventLoopConfig = identity) =
    RecordConsumerConfig(
      kafka.bootstrapServers,
      group,
      Topics(Set(topic)),
      extraProperties = fastConsumerMetadataFetching,
      offsetReset = OffsetReset.Earliest,
      eventLoopConfig = mutateEventLoop(EventLoopConfig.Default)
    )

  private def configFor(kafka: ManagedKafka, group: Group, pattern: Pattern) =
    RecordConsumerConfig(kafka.bootstrapServers, group, TopicPattern(pattern), extraProperties = fastConsumerMetadataFetching)

  private def errorMsg(offsets: Map[Partition, Seq[Offset]], expected: Map[Partition, Seq[Offset]]) =
    s"expected $expected, got $offsets"

  private def fastConsumerMetadataFetching =
    Map("metadata.max.age.ms" -> "0")

  private def aProducerRecord(topic: String) = for {
    key     <- randomId
    payload <- randomId
  } yield ProducerRecord(topic, payload, Some(key))

}

object TimedOutWaitingForMessages extends RuntimeException

case class OffsetWasAlreadyProcessed(partition: Partition, offset: Offset) extends RuntimeException
