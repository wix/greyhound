package com.wixpress.dst.greyhound.core.producer.buffered.buffers

import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import com.wixpress.dst.greyhound.core.{Headers, Partition, Topic}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration.Duration
import zio.{Chunk, ZIO}

import scala.util.Random

trait LocalBuffer {
  def failedRecordsCount: ZIO[Blocking, LocalBufferError, Int]

  def inflightRecordsCount: ZIO[Blocking, LocalBufferError, Int]

  def unsentRecordsCount: ZIO[Blocking, LocalBufferError, Int]

  def oldestUnsent: ZIO[Blocking with Clock, LocalBufferError, Long]

  def close: ZIO[Blocking, LocalBufferError, Unit]

  def enqueue(message: PersistedRecord): ZIO[Clock with Blocking, LocalBufferError, PersistedMessageId]

  def take(upTo: Int): ZIO[Clock with Blocking, LocalBufferError, Seq[PersistedRecord]]

  def delete(messageId: PersistedMessageId): ZIO[Clock with Blocking, LocalBufferError, Boolean]

  def markDead(messageId: PersistedMessageId): ZIO[Clock with Blocking, LocalBufferError, Boolean]
}

case class PersistedRecord(id: PersistedMessageId, target: SerializableTarget, encodedMsg: EncodedMessage, submitted: Long = 0L) {
  def topic: Topic = target.topic
}

case class EncodedMessage(value: Chunk[Byte], headers: Headers)

case class LocalBufferError(cause: Throwable) extends RuntimeException(cause)

case class LocalBufferFull(maxMessages: Long) extends RuntimeException(s"Local buffer has exceeded capacity. Max # of unsent messages is $maxMessages.")

case class LocalBufferProducerConfig(maxMessagesOnDisk: Long, giveUpAfter: Duration,
                                     shutdownFlushTimeout: Duration, retryInterval: Duration,
                                     strategy: ProduceStrategy = ProduceStrategy.Sync(10),
                                     localBufferBatchSize: Int = 100,
                                     id: Int = Random.nextInt(100000)) {
  def withStrategy(f: ProduceStrategy): LocalBufferProducerConfig = copy(strategy = f)

  def withMaxMessagesOnDisk(m: Int): LocalBufferProducerConfig = copy(maxMessagesOnDisk = m)

  def withGiveUpAfter(d: Duration): LocalBufferProducerConfig = copy(giveUpAfter = d)

  def withRetryInterval(d: Duration): LocalBufferProducerConfig = copy(retryInterval = d)

  def withShutdownFlushTimeout(d: Duration): LocalBufferProducerConfig = copy(shutdownFlushTimeout = d)
}

sealed trait ProduceStrategy

object ProduceStrategy {

  case class Sync(concurrency: Int) extends ProduceStrategy

  case class Async(recordsPerBatch: Int, concurrency: Int) extends ProduceStrategy

//  case object Unordered extends LocalBufferProducerFlushingStrategy

}

case class SerializableTarget(topic: Topic, partition: Option[Partition], key: Option[Chunk[Byte]])