package com.wixpress.dst.greyhound.core.producer.buffered

import java.lang.System.currentTimeMillis

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.{GreyhoundMetrics, report}
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.producer.buffered.buffers._
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import org.apache.kafka.common.errors._
import zio.Schedule.{doUntil, spaced}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.random.nextInt
import zio.stm.{STM, TRef}

import scala.util.Random

@deprecated("still work in progress - do not use this yet")
trait LocalBufferProducer {
  def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[ZEnv, LocalBufferError, BufferedProduceResult]

  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): ZIO[ZEnv, LocalBufferError, BufferedProduceResult]

  def currentState: UIO[LocalBufferProducerState]

  def shutdown: UIO[Unit]
}

case class LocalBufferProducerState(maxRecordedConcurrency: Int, running: Boolean, localBufferQueryCount: Int,
                                    failedRecords: Int) {
  def incQueryCount = copy(localBufferQueryCount = localBufferQueryCount + 1)
}

object LocalBufferProducerState {
  val empty = LocalBufferProducerState(0, running = true, localBufferQueryCount = 0, failedRecords = 0)
}

case class BufferedProduceResult(localMessageId: PersistedMessageId, kafkaResult: ZIO[ZEnv, ProducerError, RecordMetadata])

object LocalBufferProducer {
  @deprecated("still work in progress - do not use this yet")
  def make(producer: Producer, localBuffer: LocalBuffer, config: LocalBufferProducerConfig): RManaged[ZEnv with GreyhoundMetrics, LocalBufferProducer] =
    (for {
      state <- Ref.make(LocalBufferProducerState.empty)
      kafkaResponses <- TRef.makeCommit(Map.empty[PersistedMessageId, Either[ProducerError, RecordMetadata]])
      // todo i got the pendingOnBuffer mixed up!! meaning was how many on disk right now. is it still the same meaning?
      pendingOnBuffer <- TRef.makeCommit(0)
      router <- ProduceFiberRouter.make(producer, config.maxConcurrency, config.giveUpAfter)
      _ <- localBuffer.take(100).flatMap(msgs =>
        state.update(_.incQueryCount) *>
          ZIO.foreach(msgs)(msg =>
            router.produce(record(msg))
              .tapBoth(
                error => updateReferences(Left(error), pendingOnBuffer, kafkaResponses, msg) *>
                  localBuffer.markDead(msg.id),
                metadata => updateReferences(Right(metadata), pendingOnBuffer, kafkaResponses, msg) *>
                  localBuffer.delete(msg.id))
              .ignore
          ) <*
          pendingOnBuffer.update(_ - msgs.size).commit)
        .flatMap(r => ZIO.when(r.isEmpty)(pendingOnBuffer.get.map(_ > 0).flatMap(STM.check(_)).commit)) // this waits until there are more messages in buffer
        .doWhileM(_ => state.get.map(_.running))
        .flatMap(_ => localBuffer.close)
        .forkDaemon
    } yield new LocalBufferProducer {
      override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[ZEnv, LocalBufferError, BufferedProduceResult] =
        validate(config, pendingOnBuffer, state) *>
          nextInt.flatMap(generatedMsgId =>
            pendingOnBuffer.update(_ + 1).commit *>
              enqueueRecordToBuffer(localBuffer, kafkaResponses, record, generatedMsgId))


      override def produce[K, V](record: ProducerRecord[K, V], keySerializer: Serializer[K], valueSerializer: Serializer[V]): ZIO[ZEnv, LocalBufferError, BufferedProduceResult] =
        validate(config, pendingOnBuffer, state) *>
          (for {
            key <- record.key.map(k => keySerializer.serialize(record.topic, k).map(Option(_))).getOrElse(ZIO.none).mapError(LocalBufferError.apply)
            value <- valueSerializer.serialize(record.topic, record.value).mapError(LocalBufferError.apply)
            response <- produce(record.copy(key = key, value = value))
          } yield response)

      override def currentState: UIO[LocalBufferProducerState] =
        (state.get zip router.recordedConcurrency zip localBuffer.failedRecordsCount.catchAll(_ => UIO(-1))).map { case ((state, concurrency), failedRecordsCount) =>
          state.copy(maxRecordedConcurrency = concurrency, failedRecords = failedRecordsCount)
        }


      override def shutdown: UIO[Unit] =
          state.update(_.copy(running = false))
    })
      .toManaged(_.shutdown.ignore)

  private def enqueueRecordToBuffer(localBuffer: LocalBuffer, kafkaResponses: TRef[Map[PersistedMessageId, Either[ProducerError, RecordMetadata]]], record: ProducerRecord[Chunk[Byte], Chunk[Byte]], generatedMessageId: Int) = {
    localBuffer.enqueue(
      PersistedMessage(generatedMessageId,
        SerializableTarget(record.topic, record.partition, record.key),
        EncodedMessage(record.value, record.headers), currentTimeMillis))
      .map(id => BufferedProduceResult(id, kafkaResultIO(kafkaResponses, id)))
  }

  private def validate(config: LocalBufferProducerConfig, pendingOnBuffer: TRef[Int], state: Ref[LocalBufferProducerState]) =
    validateBufferFull(config, pendingOnBuffer) *> validateIsRunning(state)

  private def validateBufferFull(config: LocalBufferProducerConfig, pendingOnBuffer: TRef[Int]) =
    ZIO.whenM(pendingOnBuffer.get.commit.map(_ > config.maxMessagesOnDisk))(ZIO.fail(LocalBufferError(LocalBufferFull(config.maxMessagesOnDisk))))

  private def validateIsRunning(running: Ref[LocalBufferProducerState]) =
    ZIO.whenM(running.get.map(!_.running))(ZIO.fail(LocalBufferError(ProducerClosed())))

  private def updateReferences(result: Either[ProducerError, RecordMetadata],
                               inflightCounter: TRef[Int],
                               inflights: TRef[Map[PersistedMessageId, Either[ProducerError, RecordMetadata]]],
                               msg: PersistedMessage): URIO[Clock, Unit] =
    inflights.update(_ + (msg.id -> result))
      .commit *>
      inflights.update(_ - msg.id)
        .commit
        .delay(1.second)
        .fork
        .unit

  private def kafkaResultIO(inflights: TRef[Map[PersistedMessageId, Either[ProducerError, RecordMetadata]]],
                            id: PersistedMessageId): ZIO[Any, ProducerError, RecordMetadata] =
    waitUntilResultExists(inflights, id)
      .flatMap(result => ZIO.fromEither(result))

  private def waitUntilResultExists(inflights: TRef[Map[PersistedMessageId, Either[ProducerError, RecordMetadata]]], id: PersistedMessageId): UIO[Either[ProducerError, RecordMetadata]] =
    inflights.get.tap(map => STM.check(map.contains(id))).map(_ (id)).commit

  private def record(msg: PersistedMessage): ProducerRecord[Chunk[Byte], Chunk[Byte]] =
    ProducerRecord(msg.topic, msg.encodedMsg.value, msg.target.key, msg.target.partition, msg.encodedMsg.headers)
}

trait ProduceFiberRouter extends Producer {
  def recordedConcurrency: UIO[Int]
}

object ProduceFiberRouter {
  def make(producer: Producer, maxConcurrency: Int, giveUpAfter: Duration): URIO[ZEnv with GreyhoundMetrics, ProduceFiberRouter] =
    for {
      usedFibers <- Ref.make(Set.empty[Int])
      queues <- ZIO.foreach(0 until maxConcurrency)(i => Queue.unbounded[ProduceRequest].map(i -> _)).map(_.toMap)
      _ <- ZIO.foreach(queues.values)(
        _.take
          .flatMap((req: ProduceRequest) =>
            ZIO.whenCase(timeoutPassed(req)) {
              case true =>
                ProducerError(new TimeoutException).flip.flatMap(timeout =>
                  report(LocalBufferProduceTimeoutExceeded(req.giveUpTimestamp, System.currentTimeMillis)) *>
                    req.fail(timeout))
              case false =>
                producer.produce(req.record)
                  .tapError(error => report(LocalBufferProduceAttemptFailed(error, nonRetriable(error.getCause))))
                  .retry(spaced(1.second) && doUntil(e => timeoutPassed(req) || nonRetriable(e.getCause)))
                  .tapBoth(req.fail, req.succeed)
            }.ignore
          )
          .forever
          .forkDaemon)

    } yield new ProduceFiberRouter {

      override def recordedConcurrency: UIO[Int] = usedFibers.get.map(_.size)

      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, ZIO[Any, ProducerError, RecordMetadata]] = {
        val queueNum = Math.abs(record.key.getOrElse(Random.nextString(10)).hashCode % maxConcurrency)

        Promise.make[ProducerError, RecordMetadata].map(promise =>
          queues(queueNum).offer(ProduceRequest(record, promise, currentTimeMillis + giveUpAfter.toMillis)) *>
            usedFibers.update(_ + queueNum) *>
            promise.await)
      }
    }

  private def nonRetriable(e: Throwable): Boolean = e match {
    case _: InvalidTopicException => true
    case _: RecordBatchTooLargeException => true
    case _: UnknownServerException => true
    case _: OffsetMetadataTooLarge => true
    case _: RecordTooLargeException => true
    case _ => false
  }

  private def timeoutPassed(req: ProduceRequest): Boolean =
    currentTimeMillis > req.giveUpTimestamp
}

case class CallbackForProduceNotFound(generatedMessageId: Int) extends IllegalStateException(s"Producer callback wasn't found using the generated id: $generatedMessageId")

case class ProduceRequest(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], promise: Promise[ProducerError, RecordMetadata], giveUpTimestamp: Long) {
  def succeed(r: RecordMetadata) = promise.succeed(r)

  def fail(e: ProducerError) = promise.fail(e)
}

case class LocalBufferProduceAttemptFailed(cause: Throwable, nonRetriable: Boolean) extends GreyhoundMetric

case class LocalBufferProduceTimeoutExceeded(giveUpTimestamp: Long, currentTimestamp: Long) extends GreyhoundMetric