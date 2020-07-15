package com.wixpress.dst.greyhound.core.producer.buffered

import java.lang.System.currentTimeMillis

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.producer.buffered.buffers._
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerError, ProducerRecord, RecordMetadata}
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
}

case class LocalBufferProducerState(maxRecordedConcurrency: Int)

object LocalBufferProducerState {
  val empty = LocalBufferProducerState(0)
}

case class BufferedProduceResult(localMessageId: PersistedMessageId, kafkaResult: ZIO[ZEnv, ProducerError, RecordMetadata])

object LocalBufferProducer {

  @deprecated("still work in progress - do not use this yet")
  def make(producer: Producer, localBuffer: LocalBuffer, config: LocalBufferProducerConfig): URIO[ZEnv, LocalBufferProducer] =
    for {
      kafkaResponses <- TRef.makeCommit(Map.empty[PersistedMessageId, Either[ProducerError, RecordMetadata]])
      pendingOnBuffer <- Ref.make(0)
      router <- ProduceFiberRouter.make(producer, config.maxConcurrency, config.giveUpAfter)
      _ <- localBuffer.take(100).flatMap(msgs => {
        ZIO.foreach(msgs)(msg =>
          router.produce(record(msg))
            .tap(metadata => kafkaResponses.update(_ + (msg.id -> Right(metadata))).commit)
            .tapError(error => kafkaResponses.update(_ + (msg.id -> Left(error))).commit)
            .tap(_ => localBuffer.delete(msg.id))
            .tapBoth(_ => scheduleDeleteReference(kafkaResponses, msg), _ => scheduleDeleteReference(kafkaResponses, msg))) <*
          pendingOnBuffer.update(_ - msgs.size)
      })
        .forever
        .forkDaemon
    } yield new LocalBufferProducer {
      override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[ZEnv, LocalBufferError, BufferedProduceResult] =
        validateBufferFull(config, pendingOnBuffer) *>
          nextInt.flatMap(generatedMsgId =>
            enqueueRecordToBuffer(localBuffer, kafkaResponses, record, generatedMsgId)) <*
          pendingOnBuffer.update(_ + 1)

      override def produce[K, V](record: ProducerRecord[K, V], keySerializer: Serializer[K], valueSerializer: Serializer[V]): ZIO[ZEnv, LocalBufferError, BufferedProduceResult] =
        validateBufferFull(config, pendingOnBuffer) *>
          (for {
            key <- record.key.map(k => keySerializer.serialize(record.topic, k).map(Option(_))).getOrElse(ZIO.none).mapError(LocalBufferError.apply)
            value <- valueSerializer.serialize(record.topic, record.value).mapError(LocalBufferError.apply)
            response <- produce(record.copy(key = key, value = value))
          } yield response)

      override def currentState: UIO[LocalBufferProducerState] =
        router.recordedConcurrency.map(LocalBufferProducerState.apply)
    }

  private def enqueueRecordToBuffer(localBuffer: LocalBuffer, kafkaResponses: TRef[Map[PersistedMessageId, Either[ProducerError, RecordMetadata]]], record: ProducerRecord[Chunk[Byte], Chunk[Byte]], generatedMessageId: Int) = {
    localBuffer.enqueue(
      PersistedMessage(generatedMessageId,
        SerializableTarget(record.topic, record.partition, record.key),
        EncodedMessage(record.value, record.headers), currentTimeMillis))
      .map(id => BufferedProduceResult(id, kafkaResultIO(kafkaResponses, id)))
  }

  private def validateBufferFull(config: LocalBufferProducerConfig, pendingOnBuffer: Ref[Int]) =
    ZIO.whenM(pendingOnBuffer.get.map(_ > config.maxMessagesOnDisk))(ZIO.fail(LocalBufferError(LocalBufferFull(config.maxMessagesOnDisk))))

  private def scheduleDeleteReference(inflights: TRef[Map[PersistedMessageId, Either[ProducerError, RecordMetadata]]],
                                      msg: PersistedMessage): URIO[Clock, Unit] =
    inflights.update(_ - msg.id).commit.delay(1.second).fork.unit

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
  def make(producer: Producer, maxConcurrency: Int, giveUpAfter: Duration): URIO[ZEnv, ProduceFiberRouter] =
    for {
      usedFibers <- Ref.make(Set.empty[Int])
      queues <- ZIO.foreach(0 until maxConcurrency)(i => Queue.unbounded[ProduceRequest].map(i -> _)).map(_.toMap)
      _ <- ZIO.foreach(queues.values)(
        _.take
          .flatMap(req => producer.produceAsync(req.record)
            .retry(spaced(1.second) && doUntil(e => timeoutPassed(req) || nonRetriable(e.getCause)))
            .tapBoth(req.fail, req.succeed))
          .forever
          .forkDaemon
      )
    } yield new ProduceFiberRouter {

      override def recordedConcurrency: UIO[Int] = usedFibers.get.map(_.size)

      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, ZIO[Any, ProducerError, RecordMetadata]] = {
        val queueNum = Math.abs(record.key.getOrElse(Random.nextString(10)).hashCode % maxConcurrency)

        Promise.make[ProducerError, IO[ProducerError, RecordMetadata]].flatMap(promise =>
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

case class ProduceRequest(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], promise: Promise[ProducerError, IO[ProducerError, RecordMetadata]], giveUpTimestamp: Long) {
  def succeed(r: IO[ProducerError, RecordMetadata]) = promise.succeed(r)

  def fail(e: ProducerError) = promise.fail(e)
}