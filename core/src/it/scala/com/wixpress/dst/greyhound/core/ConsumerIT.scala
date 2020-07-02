package com.wixpress.dst.greyhound.core

import java.util.regex.Pattern

import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.OffsetReset.Earliest
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, CountDownLatch}
import com.wixpress.dst.greyhound.testkit.ITEnv._
import com.wixpress.dst.greyhound.testkit.{ITEnv, ManagedKafka}
import zio.clock.{Clock, sleep}
import zio.console.Console
import zio.duration._
import zio.{console, _}

class ConsumerIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env: UManaged[ITEnv.Env] = ITEnv.ManagedEnv

  override def sharedEnv = ITEnv.testResources()

  val resources = testResources()

  "produce and consume a single message" in {
    for {
      TestResources(kafka, producer) <- getShared
      topic <- kafka.createRandomTopic(prefix = s"topic1-single1")
      topic2 <- kafka.createRandomTopic(prefix = "topic2-single1")
      group <- randomGroup

      queue <- Queue.unbounded[ConsumerRecord[String, String]]
      handler = RecordHandler((cr: ConsumerRecord[String, String]) => UIO(println(s"***** Consumed: $cr")) *> queue.offer(cr))
        .withDeserializers(StringSerde, StringSerde)
        .ignore
      cId <- clientId
      config = configFor(kafka, group, topic).copy(clientId = cId)
      record = ProducerRecord(topic, "bar", Some("foo"))

      messages <- RecordConsumer.make(config, handler).use { consumer =>
        producer.produce(record, StringSerde, StringSerde) *>
          sleep(3.seconds) *>
          consumer.resubscribe(Set(topic, topic2)) *>
          sleep(500.millis) *> // give the consumer some time to start polling topic2
          producer.produce(record.copy(topic = topic2, value = "BAR"), StringSerde, StringSerde) *>
          (queue.take zip queue.take)
            .timeout(20.seconds)
            .tap(o => ZIO.when(o.isEmpty)(console.putStrLn("timeout waiting for messages!")))
      }
      msgs <- ZIO.fromOption(messages).mapError(_ => TimedOutWaitingForMessages)
    } yield {
      msgs._1 must (beRecordWithKey("foo") and beRecordWithValue("bar")) and (
        msgs._2 must (beRecordWithKey("foo") and beRecordWithValue("BAR")))
    }
  }

  "not lose any messages on a slow consumer (drives the message dispatcher to throttling)" in {
    for {
      TestResources(kafka, producer) <- getShared
      _ <- console.putStrLn(">>>> starting test: throttlingTest")

      topic <- kafka.createRandomTopic(partitions = 2, prefix = "core-not-lose")
      group <- randomGroup

      messagesPerPartition = 500 // Exceeds the queue capacity
      delayPartition1 <- Promise.make[Nothing, Unit]
      handledPartition0 <- CountDownLatch.make(messagesPerPartition)
      handledPartition1 <- CountDownLatch.make(messagesPerPartition)
      handler = RecordHandler { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        record.partition match {
          case 0 => handledPartition0.countDown
          case 1 => delayPartition1.await *> handledPartition1.countDown
        }
      }

      test <- RecordConsumer.make(configFor(kafka, group, topic), handler).use_ {
        val recordPartition0 = ProducerRecord(topic, Chunk.empty, partition = Some(0))
        val recordPartition1 = ProducerRecord(topic, Chunk.empty, partition = Some(1))
        for {
          _ <- ZIO.foreachPar(0 until messagesPerPartition) { _ =>
            producer.produce(recordPartition0) zipPar producer.produce(recordPartition1)
          }
          handledAllFromPartition0 <- handledPartition0.await.timeout(10.seconds)
          _ <- delayPartition1.succeed(())
          handledAllFromPartition1 <- handledPartition1.await.timeout(10.seconds)
        } yield {
          (handledAllFromPartition0 must beSome) and (handledAllFromPartition1 must beSome)
        }
      }
    } yield test
  }

  "pause and resume consumer" in {
    for {
      TestResources(kafka, producer) <- getShared
      _ <- console.putStrLn(">>>> starting test: pauseResumeTest")

      topic <- kafka.createRandomTopic(prefix = "core-pause-resume")
      group <- randomGroup

      numberOfMessages = 32
      someMessages = 16
      restOfMessages = numberOfMessages - someMessages
      handledSomeMessages <- CountDownLatch.make(someMessages)
      handledAllMessages <- CountDownLatch.make(numberOfMessages)
      handler = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        handledSomeMessages.countDown zipParRight handledAllMessages.countDown
      }

      test <- RecordConsumer.make(configFor(kafka, group, topic).copy(offsetReset = OffsetReset.Earliest), handler).use { consumer =>
        val record = ProducerRecord(topic, Chunk.empty)
        for {
          _ <- ZIO.foreachPar(0 until someMessages)(_ => producer.produce(record))
          _ <- handledSomeMessages.await
          _ <- consumer.pause
          _ <- ZIO.foreachPar(0 until restOfMessages)(_ => producer.produce(record))
          a <- handledAllMessages.await.timeout(5.seconds)
          _ <- consumer.resume
          b <- handledAllMessages.await.timeout(5.seconds)
        } yield {
          (a must beNone) and (b must beSome)
        }
      }
    } yield test
  }

  "wait until queues are drained" in {
    for {
      TestResources(kafka, producer) <- getShared
      _ <- console.putStrLn(">>>> starting test: gracefulShutdownTest")
      topic <- kafka.createRandomTopic(prefix = "core-wait-until")
      group <- randomGroup

      ref <- Ref.make(0)
      startedHandling <- Promise.make[Nothing, Unit]
      handler: Handler[Clock] = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        startedHandling.succeed(()) *>
          sleep(5.seconds) *>
          ref.update(_ + 1)
      }

      _ <- RecordConsumer.make(configFor(kafka, group, topic), handler).use_ {
        producer.produce(ProducerRecord(topic, Chunk.empty)) *>
          startedHandling.await
      }

      handled <- ref.get
    } yield {
      handled must equalTo(1)
    }
  }

  "consumer from earliest offset" in {
    for {
      TestResources(kafka, producer) <- getShared
      _ <- console.putStrLn(">>>> starting test: earliestTest")
      topic <- kafka.createRandomTopic(prefix = "core-from-earliest")
      group <- randomGroup

      queue <- Queue.unbounded[ConsumerRecord[String, String]]
      handler = RecordHandler(queue.offer(_: ConsumerRecord[String, String]))
        .withDeserializers(StringSerde, StringSerde)
        .ignore

      record = ProducerRecord(topic, "bar", Some("foo"))
      _ <- producer.produce(record, StringSerde, StringSerde)

      message <- RecordConsumer.make(configFor(kafka, group, topic).copy(offsetReset = Earliest), handler).use_ {
        queue.take
      }.timeout(10.seconds)
    } yield {
      message.get must (beRecordWithKey("foo") and beRecordWithValue("bar"))
    }
  }

  "not lose messages while throttling after rebalance" in {
    for {
      _ <- console.putStrLn(">>>> starting test: throttleWhileRebalancingTest")
      TestResources(kafka, producer) <- getShared
      partitions = 50
      topic <- kafka.createRandomTopic(partitions, prefix = "core-not-lose-while-throttling")
      group <- randomGroup
      probe <- Ref.make(Map.empty[Partition, Seq[Offset]])
      messagesPerPartition = 500
      handler = RecordHandler { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        sleep(10.millis) *>
          probe.update(curr => curr + (record.partition -> (curr.getOrElse(record.partition, Nil) :+ record.offset)))
      }

      createConsumerTask = (i: Int) => makeConsumer(kafka, topic, group, handler, i)
      test <- createConsumerTask(0).use_ {
        val record = ProducerRecord(topic, Chunk.empty, partition = Some(0))
        for {
          _ <- ZIO.foreachPar(0 until messagesPerPartition) { _ =>
            ZIO.foreachPar(0 until partitions) { p =>
              producer.produce(record.copy(partition = Some(p)))
            }
          }
          _ <- createConsumerTask(1).useForever.fork // rebalance
          _ <- createConsumerTask(2).useForever.fork // rebalance
          expected = (0 until partitions).map(p => (p, 0L until messagesPerPartition)).toMap
          _ <- probe.get.flatMap(actual =>
            if (actual != expected) ZIO.fail(errorMsg(actual, expected)) else UIO(actual))
            .retry(Schedule.duration(60.seconds) && Schedule.spaced(1.second))
        } yield ok
      }
    } yield test
  }

  "subscribe to a pattern" in {
    for {
      _ <- console.putStrLn(">>>> starting test: patternTest")
      topic = "core-subscribe-pattern-topic"
      TestResources(kafka, producer) <- getShared
      _ <- kafka.createTopic(TopicConfig(topic, 1, 1, delete))
      group <- randomGroup
      probe <- Ref.make[Option[Offset]](None)
      handler = RecordHandler { record: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
        probe.update(_ => Some(record.offset))
      }

      _ <- makeConsumer(kafka, Pattern.compile("core-subscribe-pattern.*"), group, handler, 0).use_ {
        val record = ProducerRecord(topic, Chunk.empty, key = Option(Chunk.empty))
        producer.produce(record).flatMap(_ =>
          probe.get.flatMap(actual =>
            if (!actual.contains(0))
              ZIO.fail(s"$actual != Some(0)")
            else
              UIO(actual))
            .retry(Schedule.duration(5.seconds) && Schedule.spaced(1.second))
        ).map(_ === Some(0))
      }
    } yield ok
  }


  private def makeConsumer(kafka: ManagedKafka, topic: String, group: String, handler: RecordHandler[Console with Clock, Nothing, Chunk[Byte], Chunk[Byte]], i: Int) =
    RecordConsumer.make(configFor(kafka, group, topic).copy(clientId = s"client-$i", offsetReset = OffsetReset.Earliest), handler)

  private def makeConsumer(kafka: ManagedKafka, pattern: Pattern, group: String, handler: RecordHandler[Console with Clock, Nothing, Chunk[Byte], Chunk[Byte]], i: Int) =
    RecordConsumer.make(configFor(kafka, group, pattern).copy(clientId = s"client-$i", offsetReset = OffsetReset.Earliest), handler)

  private def configFor(kafka: ManagedKafka, group: Group, topic: Topic) =
    RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic)), extraProperties = fastConsumerMetadataFetching)

  private def configFor(kafka: ManagedKafka, group: Group, pattern: Pattern) =
    RecordConsumerConfig(kafka.bootstrapServers, group, TopicPattern(pattern), extraProperties = fastConsumerMetadataFetching)

  private def errorMsg(offsets: Map[Partition, Seq[Offset]], expected: Map[Partition, Seq[Offset]]) =
    s"expected $expected, got $offsets"

  private def fastConsumerMetadataFetching =
    Map("metadata.max.age.ms" -> "0")

}

object TimedOutWaitingForMessages extends RuntimeException
