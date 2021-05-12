package com.wixpress.dst.greyhound.core.producer.buffered.buffers

import java.util.Base64

import com.wixpress.dst.greyhound.core.producer.buffered.buffers.ZChronicleQueue.ZTailer
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import com.wixpress.dst.greyhound.core.{Headers, Partition}
import net.openhft.chronicle.queue.ExcerptAppender
import net.openhft.chronicle.wire.{DocumentContext, Wire}
import zio.blocking.{Blocking, blocking}
import zio.clock.Clock
import zio.duration._
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, RManaged, Ref, Schedule, Task, UIO, URIO, ZIO}

object ChronicleQueueLocalBuffer {

  type ExcerptIndex = Long

  def make(localPath: String): RManaged[Clock with Blocking, LocalBuffer] =
    makeInternal(localPath)

  // for testing purposes
  trait ExposedLocalBuffer extends LocalBuffer {
    def getCompletionMap: Ref[Map[PersistedMessageId, Boolean]]
    def getPersistedTailer: ZTailer
  }

  def makeInternal(localPath: String): RManaged[Clock with Blocking, ExposedLocalBuffer] =
    (for {
      queue <- ZChronicleQueue.make(localPath)
      persistentMarkerTailer <- queue.createTailer("persistent").tap(t => t.peekDocument)

      enqueuedCount <- for {
        start <- persistentMarkerTailer.index
        end <- queue.createTailer >>= (_.toEnd) >>= (_.index)
        count <- queue.countExcerpts(start, end)
        ref <- Ref.make(count)
      } yield ref

      failedCounter <- Ref.make(0)

      readingTailer <- persistentMarkerTailer.copy
      unsentReportingTailer <- readingTailer.copy

      appender <- queue.acquireAppender
      completionMap <- Ref.make(Map.empty[ExcerptIndex, Boolean])

      _ <- UIO.whenM(completionMap.get.map(_.nonEmpty))(
        for {
          _ <- persistentMarkerTailer.moveToActualStart
          index <- persistentMarkerTailer.index
          deletedIndex <- completionMap.modify(scoreboard => {
            scoreboard.get(index) match {
              case Some(true) =>
                (Some(index), scoreboard - index)
              case _ =>
                (None, scoreboard)
            }
          })
          _ <- Task.whenCase(deletedIndex){case Some(idx) => persistentMarkerTailer.moveToIndex(idx + 1)}
        } yield ())
        .schedule(Schedule.spaced(100.milliseconds))
        .forkDaemon

    } yield new ExposedLocalBuffer {

      override def failedRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        failedCounter.get

      override def inflightRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        completionMap.get.map { m => m.count{case (_, completed) => !completed }}

      override def unsentRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        enqueuedCount.get.map(_.toInt)

      override def oldestUnsent: ZIO[Blocking with Clock, LocalBufferError, Option[Long]] =
        (for {
          success <- readingTailer.index.flatMap(index =>
            if (index == 0)
              readingTailer.peekDocument.flatMap(peeked =>
                if (peeked) readingTailer.index else ZIO.succeed(index))
            else
              ZIO.succeed(index))
            .flatMap(index => unsentReportingTailer.moveToIndex(index))
            .tap(moved => Task.when(moved)(unsentReportingTailer.peekDocument))
          timestamp <- if (success) unsentReportingTailer.readCurrentTimestamp else ZIO.none
        } yield timestamp)
          .mapError(LocalBufferError)

      override def close: ZIO[Blocking, LocalBufferError, Unit] = ZIO.unit

      override def enqueue(message: PersistedRecord): ZIO[Clock with Blocking, LocalBufferError, PersistedMessageId] =
        for {
          index <- blocking(RawRecord.createFrom(message).writeTo(appender).mapError(LocalBufferError))
          _ <- enqueuedCount.update(_ + 1)
        } yield index

      override def take(upTo: Int): ZIO[Clock with Blocking, LocalBufferError, Seq[PersistedRecord]] =
        (for {
          rawRecords <- ZStream.repeatEffectOption(readingTailer.readOne) // todo possibly read multiple in a managed io
            .run(ZSink.take(upTo))
            .mapError(LocalBufferError)
          _ <- ZIO.foreach(rawRecords.toList) { case (_, rawRecord) =>
            completionMap.update(m => m + (rawRecord.id -> false))
          }
          _ <- enqueuedCount.update(_ - rawRecords.size)
          records <- ZIO.foreach(rawRecords.toList) { case (_, rawRecord) => ZIO(rawRecord.toPersistedRecord) }
        } yield records).mapError(LocalBufferError)

      // todo fire off the check for the ref (TBD)
      override def delete(messageId: PersistedMessageId): ZIO[Clock with Blocking, LocalBufferError, Boolean] =
        completionMap
          .update { m =>
            m.get(messageId).map(_ => m + (messageId -> true)).getOrElse(m)
          }.map(_ => true)

      override def lastSequenceNumber: UIO[Long] = UIO(0L)

      override def markDead(messageId: PersistedMessageId): ZIO[Clock with Blocking, LocalBufferError, Boolean] =
        failedCounter.updateAndGet(_ + 1).map(_ => true)

      override def cleanup: ZIO[Blocking, LocalBufferError, Unit] =
        ZIO.unit

      override def isOpen: UIO[Boolean] =
        queue.isClosed.map(!_).catchAll(_ => UIO.succeed(false)) // silent errors!

      override def getCompletionMap: Ref[Map[PersistedMessageId, Boolean]] =
        completionMap

      override def getPersistedTailer: ZTailer =
        persistentMarkerTailer

    }).toManaged(m => m.close.ignore)

  private def encodeHeader(headers: Headers): String = {
    headers.headers
      .map { case (k, v) => encode(k.getBytes("UTF-8")) -> encode(v) }
      .map { case (k, v) => s"$k:$v" }
      .mkString(";")
  }

  case class RawRecord(id: ExcerptIndex,
                       topic: String,
                       partition: Option[Partition],
                       key: Option[Array[Byte]],
                       payload: Option[Array[Byte]],
                       headers: String,
                       timestamp: Long) {

    def toPersistedRecord: PersistedRecord =
      PersistedRecord(this.id,
        SerializableTarget(this.topic, this.partition, key.flatMap(arr => Option(arr).map(Chunk.fromArray))),
        EncodedMessage(payload.flatMap(arr => Option(arr).map(Chunk.fromArray)),
          decodeHeaders(this.headers)))

    def writeTo(appender: ExcerptAppender): Task[ExcerptIndex] = {
      ZIO(appender.writingDocument(false))
        .toManaged(dc => URIO(dc.close()))
        .use { dc =>
          writeUnmanaged(dc)
        }
    }

    private def writeUnmanaged(dc: DocumentContext): Task[ExcerptIndex] = ZIO {
      val wire: Wire = dc.wire()
      wire.write("topic").text(this.topic)
      this.partition.orElse(Some(-1)).map(wire.write("partition").int64(_))
      this.key.orElse(Some("".getBytes)).map(wire.write("key").bytes(_))
      this.payload.orElse(Some("".getBytes)).map(wire.write("payload").bytes(_))
      wire.write("headers").text(this.headers)
      wire.write("timestamp").int64(this.timestamp)
      dc.index()
    }
  }

  object RawRecord {
    def createFrom(record: PersistedRecord): RawRecord =
      RawRecord(record.id,
        record.topic,
        record.target.partition,
        record.target.key.map(_.toArray),
        record.encodedMsg.value.map(_.toArray),
        encodeHeader(record.encodedMsg.headers),
        System.currentTimeMillis())
  }

  def readFromUnsafe[R](dc: DocumentContext): ZIO[R, Throwable, (ExcerptIndex, RawRecord)] = ZIO {
    val wire = dc.wire()
    val topic = wire.read("topic").text()
    val partition = Option(wire.read("partition").int64()).flatMap(v => if (v == -1L) None else Option(v))
    val key = Option(wire.read("key").bytes()).flatMap(v => if (v sameElements "".getBytes) None else Option(v))
    val payload = Option(wire.read("payload").bytes()).flatMap(v => if (v sameElements "".getBytes) None else Option(v))
    val headers = wire.read("headers").text()
    val timestamp = wire.read("timestamp").int64()
    val index = dc.index()
    (index, RawRecord(index, topic, partition.map(_.toInt), key, payload, headers, timestamp))
  }

  private def decodeHeaders(headersString: String): Headers = {
    val map: Map[String, Chunk[Byte]] = Option(headersString).map(s => s.split(';')
      .filter(_.nonEmpty)
      .filter(_.split(":").length == 2)
      .map(part => {
        val key :: value :: Nil = part.split(":").toList
        (key, value)
      }).toMap
      .map {
        case (base64Key, base64Value) =>
          new String(decode(base64Key).toArray, "UTF-8") -> decode(base64Value)
      }).getOrElse(Map.empty)
    Headers(map)
  }

  private val encoder = Base64.getEncoder
  private val decoder = Base64.getDecoder

  def encode(bytes: Chunk[Byte]): String =
    encode(bytes.toArray)

  def encode(bytes: Array[Byte]): String =
    Option(bytes) match {
      case None => null
      case _ => new String(encoder.encode(bytes), "UTF-8")
    }

  def decode(message: String): Chunk[Byte] =
    Option(message) match {
      case None => Chunk.empty
      case _ => Chunk.fromArray(decoder.decode(message.getBytes("UTF-8")))
    }
}