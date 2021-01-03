package com.wixpress.dst.greyhound.core.producer.buffered

import java.lang.System.currentTimeMillis

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.producer.buffered.LocalBufferProducerMetric._
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.ProduceStrategy.Unordered
import com.wixpress.dst.greyhound.core.producer.buffered.buffers._
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId.notPersisted
import com.wixpress.dst.greyhound.core.{Offset, Partition, Serializer, Topic, producer}
import zio._
import zio.blocking.Blocking
import zio.clock.{Clock, sleep}
import zio.duration._
import zio.random.nextInt
import zio.stm.{STM, TRef}


trait LocalBufferProducer[R] {
  def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[ZEnv with GreyhoundMetrics with R, LocalBufferError, BufferedProduceResult]

  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V],
                    encryptor: Encryptor = NoOpEncryptor): ZIO[ZEnv with GreyhoundMetrics with R, LocalBufferError, BufferedProduceResult]

  def currentState: URIO[Blocking with Clock with R, LocalBufferProducerState]

  def close: URIO[ZEnv with GreyhoundMetrics with R, LocalBufferProducerState]
}

case class LocalBufferProducerState(maxRecordedConcurrency: Int, running: Boolean, localBufferQueryCount: Int,
                                    failedRecords: Int, enqueued: Int, inflight: Int, lagMs: Long,
                                    promises: Map[PersistedMessageId, Promise[ProducerError, RecordMetadata]]) {
  def removePromises(ids: Iterable[PersistedMessageId]): LocalBufferProducerState =
    copy(promises = promises -- ids)

  def withPromise(id: PersistedMessageId, promise: Promise[ProducerError, RecordMetadata]): LocalBufferProducerState =
    copy(promises = promises + (id -> promise))

  def removePromise(id: PersistedMessageId): LocalBufferProducerState = copy(promises = promises - id)

  def incQueryCount = copy(localBufferQueryCount = localBufferQueryCount + 1)

  def updateEnqueuedCount(by: Int) = copy(enqueued = enqueued + by)
}

object LocalBufferProducerState {
  val empty = LocalBufferProducerState(maxRecordedConcurrency = 0, running = true,
    localBufferQueryCount = 0, failedRecords = 0, enqueued = 0, inflight = 0, lagMs = 0L, promises = Map.empty)

  val invalid = LocalBufferProducerState(maxRecordedConcurrency = -1, running = false,
    localBufferQueryCount = -1, failedRecords = -1, enqueued = -1, inflight = -1, lagMs = -1L, promises = Map.empty)
}

case class BufferedProduceResult(localMessageId: PersistedMessageId, kafkaResult: IO[ProducerError, RecordMetadata])

object LocalBufferProducer {
  def make[R](producer: ProducerR[R], localBuffer: LocalBuffer, config: LocalBufferProducerConfig): RManaged[ZEnv with GreyhoundMetrics with R, LocalBufferProducer[R]] =
    (for {
      unsent <- localBuffer.unsentRecordsCount
      state <- TRef.makeCommit(LocalBufferProducerState.empty.copy(enqueued = unsent))
      router <- ProduceFlusher.make(producer, config.giveUpAfter, config.retryInterval, config.strategy)
      fiber <- ZIO.whenM(localBuffer.isOpen)(localBuffer.take(config.localBufferBatchSize)
        .timed
        .tap { case (d, msgs) => report(LocalBufferRetrievedBatch(msgs.size, d.toMillis, config.id)) }
        .map(_._2)
        .flatMap(msgs =>
          state.update(_.incQueryCount).commit *>
            ZIO.foreach(msgs)(record =>
              router.produceAsync(producerRecord(record))
                .tap(_
                  .tapBoth(
                    error => updateReferences(Left(error), state, record) *>
                      localBuffer.markDead(record.id) *>
                      report(ResilientProducerFailedNonRetriable(error, config.id, record.topic, record.id, record.submitted)),
                    metadata =>
                      updateReferences(Right(metadata), state, record) *>
                        localBuffer.delete(record.id) *>
                        report(ResilientProducerSentRecord(record.topic, metadata.partition, metadata.offset, config.id, record.id)))
                  .ignore
                  .fork)
            ).flatMap(ZIO.collectAll(_))
              .as(msgs)
        )
        .tap(r => ZIO.whenCase(r.size) {
          case 0 => state.get.flatMap(state => STM.check(state.enqueued > 0 || !state.running)).commit.delay(1.millis) // this waits until there are more messages in buffer
          case x if x <= 10 => sleep(10.millis)
        })
        .catchAllCause { e: Cause[Throwable] =>
          report(LocalBufferProducerCaughtError(e.squashTrace))
        })
        .repeatWhileM(_ => state.get.commit.map(s => s.running || s.enqueued > 0))
        .forkDaemon
    } yield new LocalBufferProducer[R] {
      override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[ZEnv with GreyhoundMetrics with R, LocalBufferError, BufferedProduceResult] =
        validate(config, state) *>
          nextInt.flatMap(generatedMsgId =>
            report(ResilientProducerAppendingMessage(record.topic, generatedMsgId, config.id)) *>
              state.update(_.updateEnqueuedCount(1)).commit *>
              enqueueRecordToBuffer(localBuffer, state, record, generatedMsgId)
                .catchAll(error => directSendToKafkaIfUnordered(router, record, config, state, error)))

      override def produce[K, V](record: ProducerRecord[K, V],
                                 keySerializer: Serializer[K],
                                 valueSerializer: Serializer[V],
                                 encryptor: Encryptor): ZIO[ZEnv with GreyhoundMetrics with R, LocalBufferError, BufferedProduceResult] =
        validate(config, state) *>
          (for {
            key <- record.key.map(k => keySerializer.serialize(record.topic, k).map(Option(_))).getOrElse(ZIO.none).mapError(LocalBufferError.apply)
            value <- valueSerializer.serializeOpt(record.topic, record.value).mapError(LocalBufferError.apply)
            serializedRecord = record.copy(key = key, value = value)
            encryptedRecord <- encryptor.encrypt(serializedRecord).mapError(LocalBufferError.apply)
            response <- produce(encryptedRecord)
          } yield response)

      override def currentState: URIO[Blocking with Clock with R, LocalBufferProducerState] =
        for {
          stateRef <- state.get.commit
          concurrency <- router.recordedConcurrency
          failedCount <- localBuffer.failedRecordsCount.catchAll(_ => UIO(-1))
          inflight <- localBuffer.inflightRecordsCount.catchAll(_ => UIO(-1))
          unsent <- localBuffer.unsentRecordsCount.catchAll(_ => UIO(-1))
          lagMs <- localBuffer.oldestUnsent.catchAll(_ => UIO(-1L))
        } yield stateRef.copy(maxRecordedConcurrency = concurrency, failedRecords = failedCount,
          enqueued = unsent, inflight = inflight, lagMs = lagMs)

      override def close: URIO[ZEnv with GreyhoundMetrics with R, LocalBufferProducerState] =
        state.modify(s => (s.enqueued + s.inflight, s.copy(running = false))).commit.flatMap(toFlush =>
          report(LocalBufferFlushingRecords(toFlush, config.id)) *>
            state.get.map(s => s.enqueued + s.inflight == 0).flatMap(STM.check(_)).commit
              .timed
              .tap(d => report(LocalBufferFlushedRecords(toFlush, d._1.toMillis, config.id)))
              .map { case (d, _) => (d, toFlush) }
              .tap(_ => fiber.join)
              .disconnect
              .timeout(config.shutdownFlushTimeout)
              .tap {
                case None => report(LocalBufferFlushTimeout(toFlush, config.shutdownFlushTimeout.toMillis, config.id))
                case _ => localBuffer.cleanup
              } *>
            (state.get.commit <* localBuffer.close)).catchAll(_ => ZIO.succeed(LocalBufferProducerState.invalid))
    })
      .toManaged((m: LocalBufferProducer[R]) => m.close.ignore)

  private def directSendToKafkaIfUnordered[R](router: ProducerR[R], record: ProducerRecord[Chunk[Byte], Chunk[Byte]], config: LocalBufferProducerConfig, state: TRef[LocalBufferProducerState], error: LocalBufferError): ZIO[Blocking with R, LocalBufferError, BufferedProduceResult] =
    config.strategy match {
      case _: Unordered => state.update(_.updateEnqueuedCount(-1)).commit *>
        router.produceAsync(record)
          .map(willComplete => BufferedProduceResult(notPersisted, willComplete))
          .catchAll(produceError => UIO(BufferedProduceResult(notPersisted, ZIO.fail(produceError))))
      case _ => ZIO.fail(error)
    }

  private def enqueueRecordToBuffer(localBuffer: LocalBuffer, state: TRef[LocalBufferProducerState],
                                    record: ProducerRecord[Chunk[Byte], Chunk[Byte]], generatedMessageId: Int): ZIO[Clock with Blocking, LocalBufferError, BufferedProduceResult] =
    Promise.make[producer.ProducerError, producer.RecordMetadata].flatMap(promise =>
      localBuffer.enqueue(persistedRecord(record, generatedMessageId))
        .tap(id => state.update(_.withPromise(id, promise)).commit)
        .map { id => BufferedProduceResult(id, promise.await) })

  private def persistedRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], generatedMessageId: Int) = {
    val target = SerializableTarget(record.topic, record.partition, record.key)
    PersistedRecord(generatedMessageId,
      target,
      EncodedMessage(record.value, record.headers), currentTimeMillis)
  }

  private def validate(config: LocalBufferProducerConfig, state: TRef[LocalBufferProducerState]) =
    validateIsRunning(state) *> validateBufferFull(config, state)

  private def validateBufferFull(config: LocalBufferProducerConfig, state: TRef[LocalBufferProducerState]) =
    ZIO.whenM(state.get.map(_.enqueued >= config.maxMessagesOnDisk).commit)(ZIO.fail(LocalBufferError(LocalBufferFull(config.maxMessagesOnDisk))))

  private def validateIsRunning(state: TRef[LocalBufferProducerState]) =
    ZIO.whenM(state.get.map(!_.running).commit)(ZIO.fail(LocalBufferError(ProducerClosed())))

  private def updateReferences(result: Either[ProducerError, RecordMetadata],
                               state: TRef[LocalBufferProducerState],
                               msg: PersistedRecord): URIO[ZEnv, Unit] =
    state.updateAndGet(_.updateEnqueuedCount(-1)).commit
      .flatMap(_.promises.get(msg.id).map(promise =>
        promise.complete(ZIO.fromEither(result)) *>
          state.update(_.removePromise(msg.id))
            .commit
            .delay(1.minutes)
            .forkDaemon)
        .getOrElse(ZIO.unit))
      .unit

  private def producerRecord(msg: PersistedRecord): ProducerRecord[Chunk[Byte], Chunk[Byte]] =
    ProducerRecord(msg.topic, msg.encodedMsg.value, msg.target.key, msg.target.partition, msg.encodedMsg.headers)
}

case class CallbackForProduceNotFound(generatedMessageId: Int) extends IllegalStateException(s"Producer callback wasn't found using the generated id: $generatedMessageId")

case class ProduceRequest(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], promise: Promise[ProducerError, RecordMetadata], giveUpTimestamp: Long) {
  def succeed(r: RecordMetadata) = promise.succeed(r)

  def fail(e: ProducerError) = promise.fail(e)
}

sealed trait LocalBufferProducerMetric extends GreyhoundMetric

object LocalBufferProducerMetric {

  case class LocalBufferProduceAttemptFailed(cause: Throwable, nonRetriable: Boolean) extends LocalBufferProducerMetric

  case class LocalBufferProduceTimeoutExceeded(giveUpTimestamp: Long, currentTimestamp: Long) extends LocalBufferProducerMetric

  case class LocalBufferFlushedRecords(recordsFlushed: Int, durationMillis: Long, id: Int) extends LocalBufferProducerMetric

  case class LocalBufferFlushTimeout(recordsFlushed: Int, timeoutMillis: Long, id: Int) extends LocalBufferProducerMetric

  case class LocalBufferFlushFinished(recordsFlushed: Int, durationMillis: Long, id: Int) extends LocalBufferProducerMetric

  case class LocalBufferFlushingRecords(recordsToFlush: Int, id: Int) extends LocalBufferProducerMetric

  case class LocalBufferRetrievedBatch(batchSize: Int, durationMillis: Long, id: Int) extends LocalBufferProducerMetric

  case class ResilientProducerAppendingMessage(topic: Topic, persistedMessageId: PersistedMessageId, id: Int) extends LocalBufferProducerMetric

  case class ResilientProducerFailedNonRetriable(cause: Throwable, producerID: Int, topic: Topic, persistentMessageID: PersistedMessageId, submitted: PersistedMessageId) extends LocalBufferProducerMetric

  case class ResilientProducerSentRecord(topic: Topic, partition: Partition, offset: Offset, producerID: Int, persistentMessageID: PersistedMessageId) extends LocalBufferProducerMetric

  case class LocalBufferProducerCaughtError(e: Throwable) extends LocalBufferProducerMetric

}


