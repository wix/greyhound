package com.wixpress.dst.greyhound.core.producer.buffered.buffers

import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import com.wixpress.dst.greyhound.core.{Headers, Partition, Topic}
import zio.clock.Clock
import zio.duration.Duration
import zio.{Chunk, IO, Task, ZIO}

trait LocalBuffer {
  def failedRecordsCount: ZIO[Any, LocalBufferError, Int]

  def close: Task[Unit]

  def enqueue(message: PersistedMessage): ZIO[Clock, LocalBufferError, PersistedMessageId]

  def take(upTo: Int): ZIO[Clock, LocalBufferError, Seq[PersistedMessage]]

  def delete(messageId: PersistedMessageId): IO[LocalBufferError, Boolean]

  def markDead(messageId: PersistedMessageId): IO[LocalBufferError, Boolean]
}

case class PersistedMessage(id: PersistedMessageId, target: SerializableTarget, encodedMsg: EncodedMessage, submitted: Long = 0L) {
  def topic: Topic = target.topic
}

case class EncodedMessage(value: Chunk[Byte], headers: Headers)

case class LocalBufferError(cause: Throwable) extends RuntimeException(cause)

case class LocalBufferFull(maxMessages: Long) extends RuntimeException(s"Local buffer has exceeded capacity. Max # of unsent messages is $maxMessages.")

case class LocalBufferProducerConfig(maxConcurrency: Int, maxMessagesOnDisk: Long, giveUpAfter: Duration) {
  def withMaxConcurrency(m: Int): LocalBufferProducerConfig = copy(maxConcurrency = m)

  def withMaxMessagesOnDisk(m: Int): LocalBufferProducerConfig = copy(maxMessagesOnDisk = m)

  def withGiveUpAfter(d: Duration): LocalBufferProducerConfig = copy(giveUpAfter = d)
}

case class SerializableTarget(topic: Topic, partition: Option[Partition], key: Option[Chunk[Byte]])