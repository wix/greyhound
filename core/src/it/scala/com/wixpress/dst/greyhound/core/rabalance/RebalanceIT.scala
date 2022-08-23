package com.wixpress.dst.greyhound.core.rabalance

import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.{CommitFailed, CommittedOffsets, PartitionsRevoked, PolledRecords}
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{topics, Topics}
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.{Blocking, InterruptibleRetryMetric, NonBlocking, RetryConfig}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, TestMetrics}
import com.wixpress.dst.greyhound.core.zioutils.CountDownLatch
import com.wixpress.dst.greyhound.core.{Group, Topic}
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testenv.ITEnv._
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import org.specs2.specification.core.Fragments
import zio.{Clock, _}
import zio.managed._

import scala.concurrent.TimeoutException

class RebalanceIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env = ITEnv.ManagedEnv

  override def sharedEnv: ZManaged[Env, Throwable, TestResources] = testResources()

  "don't reprocess messages after rebalance" in {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(prefix = "rebalance-dont-reprocess")
      group                         <- randomGroup

      allMessages  = 400
      someMessages = 100
      produce      = producer.produce(ProducerRecord(topic, Chunk.empty))

      invocations <- Ref.make(0)
      handledAll  <- CountDownLatch.make(allMessages)
      handledSome <- CountDownLatch.make(someMessages)
      handler      = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                       invocations.update(_ + 1) *> handledSome.countDown *> handledAll.countDown
                     }
      consumer     = RecordConsumer.make(configFor(topic, group, kafka).copy(offsetReset = OffsetReset.Earliest), handler)

      startProducing1 <- Promise.make[Nothing, Unit]
      consumer1       <- ZIO.scoped(consumer.tap(_ => startProducing1.succeed(()) *> handledAll.await)).fork
      _               <- startProducing1.await *> ZIO.foreachPar(0 until someMessages: Seq[Int])(_ => produce)

      _               <- handledSome.await
      startProducing2 <- Promise.make[Nothing, Unit]
      consumer2       <- ZIO.scoped(consumer.tap(_ => startProducing2.succeed(()) *> handledAll.await)).fork
      _               <- startProducing2.await *> ZIO.foreachPar(someMessages until allMessages: Seq[Int])(_ => produce)

      _              <- ZIO.foreachDiscard(Seq(consumer1, consumer2))(_.join.timeoutFail(TimeoutJoiningConsumer())(30.seconds))
      allInvocations <- invocations.get
    } yield {
      allInvocations must equalTo(allMessages)
    }
  }

  Fragments.foreach(Seq(Blocking, NonBlocking)) { retryType =>
    s"interrrupt $retryType retries on revoked partitions" in {
      for {
        r                             <- getShared
        TestResources(kafka, producer) = r
        topic                         <- kafka.createRandomTopic(prefix = "rebalance-dont-reprocess")
        group                         <- randomGroup

        metricsQueue     <- TestMetrics.queue
        retryInterrupted <- Promise.make[Nothing, Unit]

        _ <- metricsQueue.take
               .flatMap {
                 case m: PartitionsRevoked                         => ZIO.succeed(println(s">>>===>>> [${m.clientId}] $m"))
                 case d: InterruptibleRetryMetric if d.interrupted =>
                   ZIO.succeed(println(s">>>===>>> [interrupted: ${d.interrupted}] $d")) *> retryInterrupted.succeed()
                 case _                                            => ZIO.unit
               }
               .repeat(Schedule.forever)
               .fork

        produce = producer.produce(ProducerRecord(topic, Chunk.empty))

        keepConsumersAlive <- Promise.make[Nothing, Unit]
        handler             = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                                ZIO.succeed(println(s">>>===>>> boom!")) *> ZIO.fail(new RuntimeException("boom!"))
                              }
        consumer            = RecordConsumer.make(
                                configFor(topic, group, kafka).copy(
                                  offsetReset = OffsetReset.Earliest,
                                  retryConfig = retryType match {
                                    case Blocking    => Some(RetryConfig.infiniteBlockingRetry(30.seconds.asScala))
                                    case NonBlocking => Some(RetryConfig.nonBlockingRetry(30.seconds.asScala))
                                  }
                                ),
                                handler
                              )

        startProducing <- Promise.make[Nothing, Unit]
        _              <- ZIO.scoped(consumer.tap(_ => startProducing.succeed(()) *> keepConsumersAlive.await)).fork
        _              <- startProducing.await *> ZIO.foreachPar(0 until 100: Seq[Int])(_ => produce)

        _ <- ZIO.scoped(consumer.tap(_ => keepConsumersAlive.await)).fork

        _ <- retryInterrupted.await.timeoutFail(TimeoutInterruptingRetryHandler)(30.seconds)
      } yield {
        ok
      }
    }
  }

  "should successfully commit on revoke" in {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      topic                         <- kafka.createRandomTopic(partitions = 6, prefix = "revoke")
      dummy                         <- kafka.createRandomTopic(prefix = "dummy")
      produce                        = producer.produceAsync(ProducerRecord(topic, Chunk.empty))
      group                         <- randomGroup

      _ <- produce.repeat(Schedule.recurs(30)).repeat(Schedule.spaced(2.seconds).jittered).fork

      _ <- ConsumerEx
             .createAndRun(2, dummy, group, kafka) {
               case Seq(c1, c2) =>
                 c1.resubscribe(Some(topic)) *> Clock.sleep(3000.millis) *>
                   (for {
                     _ <- c2.resubscribe(Some(topic))
                     _ <- Clock.sleep(2000.millis)
                     _ <- c2.resubscribe(None)
                   } yield ()).repeat(Schedule.forever).unit
             }
             .fork

      committedInRebalance <- Promise.make[Nothing, Unit]
      failedCommits        <- Ref.make(Vector.empty[CommitFailed])

      metricsQueue <- TestMetrics.queue
      _            <- metricsQueue.take
                        .flatMap {
                          case m: CommittedOffsets if m.calledOnRebalance =>
                            ZIO.succeed(
                              println(s">>>===>>>[${m.clientId}] CommittedOffsets{ calledOnRebalance: ${m.calledOnRebalance}, offsets: ${m.offsets} }")
                            ) *> committedInRebalance.complete(ZIO.unit)
                          case m: CommitFailed                            => ZIO.succeed(println(s">>>===>>> [${m.clientId}] $m")) *> failedCommits.update(_ :+ m)
                          case m: PartitionsRevoked                       => ZIO.succeed(println(s">>>===>>> [${m.clientId}] $m"))
                          case _                                          => ZIO.unit
                        }
                        .repeat(Schedule.forever)
                        .fork

      _ <- committedInRebalance.await
             .timeoutFail(new TimeoutException("Rebalance didn't happen in 30 seconds"))(30.seconds)

      commitFailures <- failedCommits.get

    } yield {
      commitFailures must beEmpty
    }
  }

  "drain queues and finish executing tasks on rebalance" in {
    for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      group                         <- randomGroup

      partitions           = 2
      messagesPerPartition = 2
      topic               <- kafka.createRandomTopic(partitions = partitions, prefix = "rebalance-drain-queues")
      _                   <- verifyGroupCommitted(topic, group, partitions)

      latch     <- CountDownLatch.make(messagesPerPartition * partitions)
      blocker   <- Promise.make[Nothing, Unit]
      semaphore <- Semaphore.make(1)
      handler1   = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => semaphore.withPermit(blocker.await *> latch.countDown) }

      startProducing <- Promise.make[Nothing, Unit]
      config1         = configFor(topic, group, kafka).copy(
                          clientId = "client-1",
                          offsetReset = OffsetReset.Earliest,
                          eventLoopConfig = EventLoopConfig.Default.copy(
                            rebalanceListener = RebalanceListener(onRevoked = partitions => ZIO.when(partitions.nonEmpty)(blocker.succeed(())))
                          ),
                          extraProperties = fastConsumerMetadataFetching
                        )
      consumer1      <- ZIO
                          .scoped(
                            RecordConsumer
                              .make(config1, handler1)
                              .tap { _ => startProducing.succeed(()) *> latch.await }
                          )
                          .fork

      _ <- startProducing.await
      _ <- ZIO.foreachParDiscard(0 until messagesPerPartition) { _ =>
             producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(0))) zipPar
               producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(1)))
           }

      handler2   = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => latch.countDown }
      config2    = configFor(topic, group, kafka).copy(clientId = "client-2")
      consumer2 <- ZIO
                     .scoped(
                       RecordConsumer
                         .make(config2, handler2)
                         .tap { _ => latch.await }
                     )
                     .fork

      _ <- ZIO.foreachDiscard(Seq(consumer1, consumer2))(_.join.timeoutFail(TimeoutJoiningConsumer())(30.seconds))

      handled <- latch.count
    } yield {
      handled must equalTo(0)
    }
  }

  object ConsumerEx {
    def consumer(
      clientId: String,
      dummy: String,
      group: String,
      kafka: ManagedKafka,
      body: => RIO[Env, Unit]
    ): ZIO[Env with RecordConsumer.Env with Scope, Throwable, RecordConsumer[Env with RecordConsumer.Env]] =
      RecordConsumer.make(
        configFor(dummy, group, kafka).copy(offsetReset = OffsetReset.Earliest, clientId = clientId),
        RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => body }
      )

    def createAndRun(count: Int, dummy: String, group: String, kafka: ManagedKafka)(
      f: Seq[ConsumerEx] => RIO[Env, Any]
    ): RIO[Env with RecordConsumer.Env, Any] =
      ZIO.scoped {
        for {
          counters <- ZIO.foreach(Range(0, count).toArray)(_ => Ref.make(0))
          _        <- ZIO
                        .foreach(Range(0, count).toList) { index =>
                          consumer(s"consumer-$index", dummy, group, kafka, counters(index).update(_ + 1))
                            .map { c => new ConsumerEx(s"consumer-$index", dummy, counters(index), c) }
                        }
                        .tap(f)
        } yield ()
      }.unit
  }

  class ConsumerEx(val clientId: String, dummy: String, consumedCounter: Ref[Int], consumer: RecordConsumer[_]) {
    def consumed                           = consumedCounter.get
    def resubscribe(topic: Option[String]) = {
      ZIO.succeed(println(s"+++ $clientId ${topic.fold("unsubscribing")(_ => s"subscribing")}")) *>
        consumer.resubscribe(topics(topic.toSeq :+ dummy: _*)).flatMap { assigned =>
          val partitions = topic.toSet.flatMap((t: String) =>
            assigned.collect {
              case tp if tp.topic == t => tp.partition
            }
          )
          val msg        = topic.fold("unsubscribed") { _ => s"subscribed, assigned: ${partitions.mkString(", ")}" }
          ZIO.succeed(println(s"+++ $clientId $msg"))
        }
    }
  }

  private def recordCount(m: PolledRecords) = {
    m.records.foldLeft(0) {
      case (acc, (_, seq)) =>
        acc +
          seq.foldLeft(0) {
            case (acc, (_, seq)) =>
              acc + seq.size
          }
    }
  }

  private def verifyGroupCommitted(topic: Topic, group: Group, partitions: Int) =
    ZIO.scoped(for {
      r                             <- getShared
      TestResources(kafka, producer) = r
      latch                         <- CountDownLatch.make(partitions)
      handler                        = RecordHandler((_: ConsumerRecord[Chunk[Byte], Chunk[Byte]]) => latch.countDown)
      _                             <- RecordConsumer.make(configFor(topic, group, kafka), handler).tap { _ =>
                                         ZIO.foreachParDiscard(0 until partitions) { partition =>
                                           producer.produce(ProducerRecord(topic, Chunk.empty, partition = Some(partition)))
                                         } *> latch.await
                                       }
    } yield ())

  private def configFor(topic: Topic, group: Group, kafka: ManagedKafka) =
    RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic)), extraProperties = fastConsumerMetadataFetching)

  private def fastConsumerMetadataFetching = Map("metadata.max.age.ms" -> "0")
}

case class TimeoutJoiningConsumer()          extends RuntimeException
case class TimeoutInterruptingRetryHandler() extends RuntimeException
