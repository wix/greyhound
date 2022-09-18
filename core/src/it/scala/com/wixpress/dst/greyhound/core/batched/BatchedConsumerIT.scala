package com.wixpress.dst.greyhound.core.consumer.batched

import java.time.Duration

import com.wixpress.dst.greyhound.core.Serdes.StringSerde
import com.wixpress.dst.greyhound.core.consumer.OffsetReset
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.{Group, Topic}
import com.wixpress.dst.greyhound.core.consumer.domain.{BatchRecordHandler, ConsumerRecord, ConsumerRecordBatch, ConsumerSubscription, HandleError}
import com.wixpress.dst.greyhound.core.producer.{ProducerRecord, ReportingProducer}
import com.wixpress.dst.greyhound.core.testkit.{AwaitableRef, BaseTestWithSharedEnv}
import com.wixpress.dst.greyhound.core.zioutils.CountDownLatch
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers.{beRecordWithKey, beRecordWithValue}
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import com.wixpress.dst.greyhound.testenv.ITEnv.{clientId, _}
import org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG
import zio.{Chunk, Queue, Ref, ZIO}

import zio.{Console, _}
import zio.Clock.sleep

class BatchedConsumerIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env = ITEnv.ManagedEnv

  override def sharedEnv = ITEnv.testResources()

  val resources = testResources()

  "produce, consume and resubscribe" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(prefix = s"topic1-single1")
        topic2                        <- kafka.createRandomTopic(prefix = "topic2-single1")
        group                         <- randomGroup

        queue    <- Queue.unbounded[ConsumerRecord[String, String]]
        handler   = BatchRecordHandler((cr: ConsumerRecordBatch[String, String]) =>
                      ZIO.succeed(println(s"***** Consumed: $cr")) *> ZIO.foreach(cr.records)(queue.offer)
                    ).withDeserializers(StringSerde, StringSerde)
        cId      <- clientId
        config    = configFor(kafka, group, topic).copy(clientId = cId)
        records1  = producerRecords(topic, "1", partitions, 5)
        records2  = producerRecords(topic, "2", partitions, 5)
        messages <- BatchConsumer.make(config, handler).flatMap { consumer =>
                      val totalExpected = records1.size + records2.size
                      produceBatch(producer, records1) *> sleep(3.seconds) *>
                        consumer.resubscribe(ConsumerSubscription.topics(topic, topic2)) *>
                        sleep(500.millis) *> // give the consumer some time to start polling topic2
                        produceBatch(producer, records2) *>
                        queue
                          .takeBetween(totalExpected, totalExpected)
                          .timeout(20.seconds)
                          .tap(o => ZIO.when(o.isEmpty)(Console.printLine("timeout waiting for messages!")))
                    }
        msgs     <- ZIO.fromOption(messages).orElseFail(TimedOutWaitingForMessages)
      } yield {
        msgs must
          allOf(
            (records1 ++ records2).map(r => beRecordWithKey(r.key.get) and beRecordWithValue(r.value.get)): _*
          )
      }
    }

  "be able to resubscribe to same topics" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        partitions                     = 1
        topic                         <- kafka.createRandomTopic(prefix = s"topic1-single1", partitions = partitions)
        group                         <- randomGroup

        queue    <- Queue.unbounded[ConsumerRecord[String, String]]
        handler   = BatchRecordHandler((cr: ConsumerRecordBatch[String, String]) =>
                      ZIO.succeed(println(s"***** Consumed: $cr")) *> ZIO.foreach(cr.records)(queue.offer)
                    ).withDeserializers(StringSerde, StringSerde)
        cId      <- clientId
        config    = configFor(kafka, group, topic, resubscribeTimeout = 1.second).copy(clientId = cId)
        records1  = producerRecords(topic, "1", partitions, 5)
        records2  = producerRecords(topic, "2", partitions, 5)
        messages <- BatchConsumer.make(config, handler).flatMap { consumer =>
                      val totalExpected = records1.size + records2.size
                      produceBatch(producer, records1) *> sleep(3.seconds) *> consumer.resubscribe(ConsumerSubscription.topics(topic)) *>
                        produceBatch(producer, records2) *> sleep(3.seconds) *>
                        queue
                          .takeBetween(totalExpected, totalExpected)
                          .timeout(20.seconds)
                          .tap(o => ZIO.when(o.isEmpty)(Console.printLine("timeout waiting for messages!")))
                    }
        msgs     <- ZIO.fromOption(messages).orElseFail(TimedOutWaitingForMessages)
      } yield {
        msgs must
          allOf(
            (records1 ++ records2).map(r => beRecordWithKey(r.key.get) and beRecordWithValue(r.value.get)): _*
          )
      }
    }

  "handle errors with retry" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(prefix = "handler-fails")
        group                         <- randomGroup
        failsOn                       <- Ref.make[Seq[ConsumerRecord[String, String]] => Boolean](_ => false)
        handled                       <- AwaitableRef.make(Seq.empty[ConsumerRecord[String, String]])
        failed                        <- AwaitableRef.make(Seq.empty[ConsumerRecord[String, String]])

        handler      = BatchRecordHandler((cr: ConsumerRecordBatch[String, String]) =>
                         ZIO.whenZIO(failsOn.get.map(_(cr.records)))(
                           failed.update(_ ++ cr.records) *> ZIO.succeed(println(s"failing for records: $cr")) *>
                             ZIO.fail(HandleError(new RuntimeException))
                         ) *> ZIO.succeed(println(s"handled records: $cr")) *> handled.update(_ ++ cr.records)
                       ).withDeserializers(StringSerde, StringSerde)
        retryBackoff = 1.second
        config       = configFor(kafka, group, topic).copy(offsetReset = OffsetReset.Earliest, retryConfig = Some(BatchRetryConfig(retryBackoff)))
        test        <- BatchConsumer.make(config, handler).flatMap { _ =>
                         val recsPerPartition = 10
                         val records1         = producerRecords(topic, "1", partitions, recsPerPartition)
                         val records2         = producerRecords(topic, "2", partitions, recsPerPartition)
                         val failingPartition = 0
                         for {
                           _                  <- failsOn.set(_.head.partition == failingPartition)
                           _                  <- ZIO.succeed(println(s"producing 1st set of ${records1.size} records"))
                           _                  <- produceBatch(producer, records1)
                           _                  <- ZIO.succeed(s"produced ${records1.size} records")
                           failed1            <- failed.await(_.nonEmpty, 5.seconds)
                           handled1           <- handled.await(_.size >= recsPerPartition * (partitions - 1), 10.seconds)
                           _                  <- ZIO.succeed(println(s"producing 2nd set of ${records2.size} records"))
                           _                  <- produceBatch(producer, records2)
                           _                  <- ZIO.succeed(println(s"sleeping for ${retryBackoff * 4}"))
                           _                  <- sleep(retryBackoff * 4)
                           _                  <- ZIO.succeed(println(s"done sleeping for ${retryBackoff * 4}"))
                           failedAfterRetries <- failed.get
                           handled2           <- handled.await(_.size >= (recsPerPartition * (partitions - 1)) * 2, 10.seconds)
                           _                  <- failsOn.set(_ => false)
                           handled3           <- handled.await(_.size >= records1.size + records2.size, 10.seconds)
                         } yield {
                           failedAfterRetries.size must beGreaterThanOrEqualTo(failed1.size * 3)
                           consumerRecValuesByPartition(handled1) === producerRecValuesByPartition(records1) - failingPartition
                           consumerRecValuesByPartition(handled2) === producerRecValuesByPartition(records1 ++ records2) - failingPartition
                           consumerRecValuesByPartition(handled3) === producerRecValuesByPartition(records1 ++ records2)
                         }
                       }
      } yield test
    }

  "pause and resume consumer" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        _                             <- Console.printLine(">>>> starting test: pauseResumeTest")

        topic <- kafka.createRandomTopic(prefix = "core-pause-resume")
        group <- randomGroup

        numberOfMessages     = 32
        someMessages         = 16
        restOfMessages       = numberOfMessages - someMessages
        handledSomeMessages <- CountDownLatch.make(someMessages)
        handledAllMessages  <- CountDownLatch.make(numberOfMessages)
        handleCounter       <- Ref.make[Int](0)
        handler              = BatchRecordHandler { recs: ConsumerRecordBatch[Chunk[Byte], Chunk[Byte]] =>
                                 ZIO.succeed(println(s"consumed ${recs.size} messages for partition ${recs.records.head.partition}")) *>
                                   handleCounter.update(_ + recs.size) *> handledSomeMessages.countDown(recs.size) zipParRight
                                   handledAllMessages.countDown(recs.size)
                               }

        config = configFor(kafka, group, topic).copy(offsetReset = OffsetReset.Earliest)
        test  <- BatchConsumer.make(config, handler).flatMap { consumer =>
                   val record = ProducerRecord(topic, Chunk.empty)
                   for {
                     _                 <- ZIO.foreachParDiscard(0 until someMessages)(_ => producer.produceAsync(record))
                     _                 <- handledSomeMessages.await
                     _                 <- consumer.pause
                     _                 <- sleep(config.eventLoopConfig.pollTimeout + 1.second) // make sure last poll finished
                     _                 <- ZIO.foreachParDiscard(0 until restOfMessages)(_ => producer.produceAsync(record))
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

  "rebalance between two consumers" in
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(prefix = "rebalance")
        group                         <- randomGroup

        consumedKeys <- AwaitableRef.make(Seq.empty[String])
        records       = producerRecords(topic, "1", partitions, 50)
        barrier      <- Queue.bounded[Unit](1)
        handler       =
          (id: String) =>
            BatchRecordHandler((cr: ConsumerRecordBatch[String, String]) =>
              consumedKeys.get.map(msgs =>
                println(s"***** [#${cr.partition}][$id] Consumed: ${cr.size} (total: ${msgs.size + cr.size})")
              ) *>
                barrier
                  .offer(())
                  // we can't block here, otherwise rebalance won't happen - so we just fail
                  .timeoutFail(new RuntimeException("queue full"))(1.second)
                  .tapError(_ => ZIO.succeed(println(s"[$id] timed out waiting on barrier")))
                  .orDie *> consumedKeys.update(_ ++ cr.records.map(_.key.get))
            ).withDeserializers(StringSerde, StringSerde)
        cId1         <- clientId("c1")
        cId2         <- clientId("c2")
        _            <- ZIO.succeed(println(s"**** starting consumer $cId1"))
        config        = configFor(kafka, group, topic, extraProperties = Map(MAX_POLL_RECORDS_CONFIG -> "10"), retryWithBackoff = Some(500.millis))
                          .copy(clientId = cId1)
        _            <- BatchConsumer
                          .make(config, handler(cId1))
                          .flatMap { _ =>
                            ZIO.succeed(println(s"*** producing ${records.size} records")) *> produceBatch(producer, records) *>
                              ZIO.succeed(println(s"**** awaiting handled batch on each of $partitions partitions")) *>
                              consumedKeys.await(_.size >= 0) *> ZIO.succeed(println(s"**** starting second consumer $cId2")) *>
                              BatchConsumer.make(config.copy(clientId = cId2), handler(cId2)).flatMap { _ => barrier.take.repeatWhile(_ => true) }
                          }
                          .fork
        msgs         <- consumedKeys.await(_.size >= records.size, 20.seconds)
      } yield {
        msgs.sorted === records.map(_.key.get).sorted
      }
    }

  private def configFor(
    kafka: ManagedKafka,
    group: Group,
    topic: Topic,
    mutateEventLoop: BatchEventLoopConfig => BatchEventLoopConfig = identity,
    extraProperties: Map[String, String] = Map.empty,
    retryWithBackoff: Option[Duration] = None,
    resubscribeTimeout: Duration = 30.seconds
  ) =
    BatchConsumerConfig(
      kafka.bootstrapServers,
      group,
      Topics(Set(topic)),
      extraProperties = extraProperties ++ fastConsumerMetadataFetching,
      offsetReset = OffsetReset.Earliest,
      eventLoopConfig = mutateEventLoop(BatchEventLoopConfig.Default),
      retryConfig = retryWithBackoff.map(BatchRetryConfig.apply),
      resubscribeTimeout = resubscribeTimeout
    )

  private def fastConsumerMetadataFetching =
    Map("metadata.max.age.ms" -> "0")

  private def consumerRecValuesByPartition(cr: Seq[ConsumerRecord[_, String]]) = {
    cr.groupBy(_.partition).mapValues(_.map(_.value)).toMap
  }

  private def producerRecValuesByPartition(cr: Seq[ProducerRecord[_, String]]) = {
    cr.groupBy(_.partition.get).mapValues(_.map(_.value.get)).toMap
  }

  private def producerRecords(topic: String, tag: String, partitions: Int, recsPerPartition: Int) = {
    (0 until partitions).flatMap(i =>
      (0 until recsPerPartition).map(j => {
        val jPadded = j.toString.reverse.padTo(2, '0').reverse
        ProducerRecord(topic, s"value-t$tag-p$i-$jPadded", Some(s"key-t$tag-p$i-$jPadded"), partition = Some(i))
      })
    )
  }

  def produceBatch(producer: ReportingProducer[Any], records: Seq[ProducerRecord[String, String]]) =
    ZIO
      .foreach(records)(r => producer.produceAsync(r, StringSerde, StringSerde))
      .flatMap(ZIO.collectAll(_))

}

object TimedOutWaitingForMessages extends RuntimeException
