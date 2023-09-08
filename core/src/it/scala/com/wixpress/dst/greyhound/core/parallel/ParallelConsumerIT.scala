package com.wixpress.dst.greyhound.core.parallel
import com.wixpress.dst.greyhound.core.Serdes.StringSerde
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.SkippedGapsOnInitialization
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, ConsumerSubscription, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.{EventLoopConfig, RebalanceListener, RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.producer.{ProducerRecord, ReportingProducer}
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers.{beRecordWithKey, beRecordWithValue, beRecordsWithKeysAndValues}
import com.wixpress.dst.greyhound.core.testkit.{eventuallyZ, BaseTestWithSharedEnv, TestMetrics}
import com.wixpress.dst.greyhound.core.zioutils.CountDownLatch
import com.wixpress.dst.greyhound.core.{Group, Topic, TopicPartition}
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testenv.ITEnv.{clientId, partitions, randomGroup, randomId, Env, ManagedKafkaOps, TestResources}
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import zio.Clock.sleep
import zio.{Queue, ZIO, _}

class ParallelConsumerIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env = ITEnv.ManagedEnv

  override def sharedEnv = ITEnv.testResources()

  "consume messages correctly after rebalance" in {
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic1                        <- kafka.createRandomTopic(prefix = "topic1")
        topic2                        <- kafka.createRandomTopic(prefix = "topic2")
        group                         <- randomGroup

        queue             <- Queue.unbounded[ConsumerRecord[String, String]]
        handler            = RecordHandler((cr: ConsumerRecord[String, String]) => queue.offer(cr)).withDeserializers(StringSerde, StringSerde)
        cId               <- clientId
        config             = parallelConsumerConfig(kafka, topic1, group, cId)
        records1           = producerRecords(topic1, "1", partitions, 10)
        records2           = producerRecords(topic1, "2", partitions, 10)
        numRecordsExpected = records1.size + records2.size
        messagesOption    <- for {
                               consumer      <- RecordConsumer.make(config, handler)
                               _             <- produceRecords(producer, records1)
                               _             <- sleep(5.seconds)
                               _             <- consumer.resubscribe(ConsumerSubscription.topics(topic1, topic2)) // trigger rebalance
                               _             <- sleep(500.millis)
                               _             <- produceRecords(producer, records2)
                               maybeMessages <- queue
                                                  .takeBetween(numRecordsExpected, numRecordsExpected)
                                                  .timeout(60.seconds)
                                                  .tap(o => ZIO.when(o.isEmpty)(Console.printLine("timeout waiting for messages")))
                             } yield maybeMessages
        messages          <- ZIO.fromOption(messagesOption).orElseFail(TimedOutWaitingForMessages)
      } yield {
        messages must beRecordsWithKeysAndValues(records1 ++ records2)
      }
    }
  }

  "consume messages exactly once when processing following multiple consecutive polls" in {
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic()
        group                         <- randomGroup
        queue                         <- Queue.unbounded[ConsumerRecord[String, String]]
        handlerWithSleep               =
          RecordHandler((cr: ConsumerRecord[String, String]) => {
            (if (cr.partition == cr.offset) ZIO.sleep(2.seconds) // sleep to simulate long processing time and go through multiple polls
             else ZIO.unit) *> queue.offer(cr)
          })
            .withDeserializers(StringSerde, StringSerde)
        cId                           <- clientId
        config                         = parallelConsumerConfig(kafka, topic, group, cId)
        records                        = producerRecords(topic, "1", partitions, 5)
        messagesOption                <- RecordConsumer.make(config, handlerWithSleep).flatMap { consumer =>
                                           produceRecords(producer, records) *> ZIO.sleep(3.seconds) *>
                                             queue
                                               .takeBetween(records.size, records.size)
                                               .timeout(60.seconds)
                                               .tap(o => ZIO.when(o.isEmpty)(Console.printLine("timeout waiting for messages!")))
                                         }
        messages                      <- ZIO.fromOption(messagesOption).orElseFail(TimedOutWaitingForMessages)
      } yield {
        messages must
          allOf(
            records.map(r => beRecordWithKey(r.key.get) and beRecordWithValue(r.value.get)): _*
          )
      }
    }
  }

  "consume gaps after rebalance and skip already-consumed records" in {
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(partitions = 1)
        group                         <- randomGroup
        cId                           <- clientId
        partition                      = 0
        allMessages                    = 10
        fastMessages                   = allMessages - 1
        drainTimeout                   = 5.seconds

        numProcessedMessages <- Ref.make[Int](0)
        fastMessagesLatch    <- CountDownLatch.make(fastMessages)

        randomKeys <- ZIO.foreach(1 to fastMessages)(i => randomKey(i.toString)).map(_.toSeq)

        fastRecords = randomKeys.map { key => recordWithKey(topic, key, partition) }
        slowRecord  = recordWithoutKey(topic, partition)

        finishRebalance <- Promise.make[Nothing, Unit]

        // handler that sleeps only on the slow key
        handler   = RecordHandler { cr: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                      (cr.key match {
                        case Some(_) =>
                          fastMessagesLatch.countDown
                        case None    =>
                          // make sure the handler doesn't finish before the rebalance is done, including drain timeout
                          finishRebalance.await *> ZIO.sleep(drainTimeout + 5.second)
                      }) *> numProcessedMessages.update(_ + 1)
                    }
        consumer <- makeParallelConsumer(handler, kafka, topic, group, cId, drainTimeout = drainTimeout, startPaused = true)
        _        <- produceRecords(producer, Seq(slowRecord))
        _        <- produceRecords(producer, fastRecords)
        _        <- ZIO.sleep(2.seconds)
        // produce is done synchronously to make sure all records are produced before consumer starts, so all records are polled at once
        _        <- consumer.resume
        _        <- fastMessagesLatch.await
        _        <- ZIO.sleep(3.second) // sleep to ensure commit is done before rebalance
        // start another consumer to trigger a rebalance before slow handler is done
        _        <- makeParallelConsumer(
                      handler,
                      kafka,
                      topic,
                      group,
                      cId,
                      drainTimeout = drainTimeout,
                      onAssigned = _ => finishRebalance.succeed()
                    )

        _ <- eventuallyZ(numProcessedMessages.get, 25.seconds)(_ == allMessages)
      } yield {
        ok
      }
    }
  }

  "migrate correctly from regular record consumer to parallel consumer - consume every record once" in {
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(partitions = 1)
        group                         <- randomGroup
        cId                           <- clientId
        tp                             = TopicPartition(topic, 0)

        regularConfig      = configFor(kafka, group, Set(topic))
        parallelConfig     = parallelConsumerConfig(kafka, topic, group, cId) // same group name for both consumers
        handledOffsets    <- Ref.make[Seq[Long]](Seq.empty)                   // keep track of handled offsets to make sure no duplicates are processed
        regularHandler     = RecordHandler((cr: ConsumerRecord[String, String]) => handledOffsets.update(_ :+ cr.offset))
                               .withDeserializers(StringSerde, StringSerde)
        longRunningHandler = RecordHandler((cr: ConsumerRecord[String, String]) =>
                               (if (cr.offset % 2 == 0) ZIO.sleep(2.seconds) else ZIO.unit) *> handledOffsets.update(_ :+ cr.offset)
                             ).withDeserializers(StringSerde, StringSerde)

        records1 = producerRecords(topic, "1", 1, 10)
        records2 = producerRecords(topic, "2", 1, 10)
        records3 = producerRecords(topic, "3", 1, 10)
        records4 = producerRecords(topic, "4", 1, 10)

        regularConsumer1  <- RecordConsumer.make(regularConfig, regularHandler)
        _                 <- produceRecords(producer, records1)
        _                 <- eventuallyZ(handledOffsets.get)(_.sorted == records1.indices.map(_.toLong).sorted)
        _                 <- regularConsumer1.shutdown()
        parallelConsumer1 <- RecordConsumer.make(parallelConfig, longRunningHandler)
        parallelConsumer2 <- RecordConsumer.make(parallelConfig, longRunningHandler)
        _                 <- produceRecords(producer, records2)
        _                 <- eventuallyZ(handledOffsets.get, timeout = 10.seconds)(_.sorted == (records1 ++ records2).indices.map(_.toLong).sorted)
        parallelConsumer3 <- RecordConsumer.make(parallelConfig, longRunningHandler).delay(5.seconds)
        _                 <- parallelConsumer1.shutdown() zipPar parallelConsumer2.shutdown()
        _                 <- produceRecords(producer, records3)
        _                 <-
          eventuallyZ(handledOffsets.get, timeout = 10.seconds)(_.sorted == (records1 ++ records2 ++ records3).indices.map(_.toLong).sorted)
        _                 <- produceRecords(producer, records4)
        _                 <- eventuallyZ(handledOffsets.get, timeout = 10.seconds)(
                               _.sorted == (records1 ++ records2 ++ records3 ++ records4).indices.map(_.toLong).sorted
                             )
        _                 <- eventuallyZ(parallelConsumer3.committedOffsetsAndGaps(Set(tp)), timeout = 10.seconds)(_.get(tp).exists(_.gaps.isEmpty))
      } yield ok
    }
  }

  "migrate from parallel consumer with gaps to regular consumer - consume from latest and report non-consumed gaps" in {
    ZIO.scoped {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic()
        group                         <- randomGroup
        cId                           <- clientId
        partition                      = 0
        allMessages                    = 10
        fastMessages                   = allMessages - 1

        skippedGaps  <- Ref.make[Int](0)
        metricsQueue <- TestMetrics.queue

        regularConfig = configFor(kafka, group, Set(topic))
        _            <- metricsQueue.take
                          .flatMap {
                            case _: SkippedGapsOnInitialization => skippedGaps.update(_ + 1)
                            case _                              => ZIO.unit
                          }
                          .repeat(Schedule.forever)
                          .fork

        keyWithSlowHandling   = "slow-key"
        numProcessedMessages <- Ref.make[Int](0)
        fastMessagesLatch    <- CountDownLatch.make(fastMessages)

        randomKeys <- ZIO.foreach(1 to fastMessages)(i => randomKey(i.toString)).map(_.toSeq)

        fastRecords       = randomKeys.map { key => recordWithKey(topic, key, partition) }
        slowRecord        = recordWithKey(topic, keyWithSlowHandling, partition)
        additionalRecords = producerRecords(topic, "additional", 1, 5)

        finishRebalance <- Promise.make[Nothing, Unit]

        // handler that sleeps forever on the slow key
        parallelConsumerHandler = RecordHandler { cr: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                                    (cr.key match {
                                      case Some(k) if k == Chunk.fromArray(keyWithSlowHandling.getBytes) =>
                                        ZIO.sleep(Duration.Infinity)
                                      case _                                                             => fastMessagesLatch.countDown
                                    }) *> numProcessedMessages.update(_ + 1)
                                  }

        regularConsumerHandler = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => numProcessedMessages.update(_ + 1) }

        parallelConsumer <- makeParallelConsumer(parallelConsumerHandler, kafka, topic, group, cId, startPaused = true)
        _                <- produceRecords(producer, Seq(slowRecord))
        _                <- produceRecords(producer, fastRecords)
        _                <- ZIO.sleep(3.seconds)
        // produce is done synchronously to make sure all records are produced before consumer starts, so all records are polled at once
        _                <- parallelConsumer.resume
        _                <- fastMessagesLatch.await
        _                <- ZIO.sleep(2.second) // sleep to ensure commit is done before rebalance
        // migrate to regular fromLatest consumer while gap exists
        _                <- parallelConsumer.shutdown() *> RecordConsumer.make(regularConfig, regularConsumerHandler)
        _                <- produceRecords(producer, additionalRecords)
        _                <- eventuallyZ(numProcessedMessages.get, 20.seconds)(_ == fastMessages + additionalRecords.size)
        _                <- eventuallyZ(skippedGaps.get, 20.seconds)(_.must(beGreaterThanOrEqualTo(1)))
      } yield {
        ok
      }
    }
  }

  private def configFor(
    kafka: ManagedKafka,
    group: Group,
    topics: Set[Topic],
    mutateEventLoop: EventLoopConfig => EventLoopConfig = identity,
    extraProperties: Map[String, String] = Map.empty
  ) = RecordConsumerConfig(
    bootstrapServers = kafka.bootstrapServers,
    group = group,
    initialSubscription = Topics(topics),
    eventLoopConfig = mutateEventLoop(EventLoopConfig.Default),
    extraProperties = extraProperties
  )

  private def makeParallelConsumer(
    handler: RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]],
    kafka: ManagedKafka,
    topic: String,
    group: String,
    cId: String,
    drainTimeout: Duration = 20.seconds,
    startPaused: Boolean = false,
    onAssigned: Set[TopicPartition] => UIO[Any] = _ => ZIO.unit
  ) =
    RecordConsumer.make(parallelConsumerConfig(kafka, topic, group, cId, drainTimeout, startPaused, onAssigned), handler)

  private def parallelConsumerConfig(
    kafka: ManagedKafka,
    topic: String,
    group: String,
    cId: String,
    drainTimeout: Duration = 20.seconds,
    startPaused: Boolean = false,
    onAssigned: Set[TopicPartition] => UIO[Any] = _ => ZIO.unit
  ) = {
    configFor(
      kafka,
      group,
      Set(topic),
      mutateEventLoop = _.copy(
        consumePartitionInParallel = true,
        maxParallelism = 10,
        drainTimeout = drainTimeout,
        startPaused = startPaused,
        rebalanceListener = RebalanceListener(onAssigned = onAssigned)
      )
    )
      .copy(clientId = cId)
  }

  private def producerRecords(topic: String, tag: String, partitions: Int, recordsPerPartition: Int) = (0 until partitions).flatMap(p =>
    (0 until recordsPerPartition).map(i => ProducerRecord(topic, s"value-t$tag-p$p-$i", Some(s"key-t$tag-p$p-$i"), partition = Some(p)))
  )

  def produceRecords(producer: ReportingProducer[Any], records: Seq[ProducerRecord[String, String]]) =
    ZIO
      .foreach(records)(r => producer.produce(r, StringSerde, StringSerde))

  private def recordWithKey(topic: String, key: String, partition: Int) =
    ProducerRecord(topic, "", Some(key), partition = Some(partition))

  private def recordWithoutKey(topic: String, partition: Int) =
    ProducerRecord(topic, "", None, partition = Some(partition))

  private def randomKey(prefix: String) =
    randomId.map(r => s"$prefix-$r")
}

object TimedOutWaitingForMessages extends RuntimeException
