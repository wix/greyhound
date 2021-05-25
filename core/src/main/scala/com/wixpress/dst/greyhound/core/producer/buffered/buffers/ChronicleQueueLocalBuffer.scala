package com.wixpress.dst.greyhound.core.producer.buffered.buffers

import java.util.Base64

import com.wixpress.dst.greyhound.core.producer.buffered.buffers.ChronicleQueueLocalBuffer.RawRecord.partitionExists
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.ZChronicleQueue.ZTailer
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import com.wixpress.dst.greyhound.core.{Headers, Partition}
import net.openhft.chronicle.bytes.Bytes
import net.openhft.chronicle.queue.ExcerptAppender
import zio.blocking.{Blocking, blocking}
import zio.clock.Clock
import zio.duration._
import zio.json._
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, RManaged, Ref, Schedule, Semaphore, Task, UIO, ZIO}

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
      persistentMarkerTailer <- queue.createTailer("persistent").tap(t => t.moveToActualStart)

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

      writeSem <- Semaphore.make(1)
      readSem <- Semaphore.make(1)

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
          _ <- Task.whenCase(deletedIndex) { case Some(idx) => persistentMarkerTailer.moveToIndex(idx + 1) }
        } yield ())
        .schedule(Schedule.spaced(100.milliseconds))
        .forkDaemon
    } yield new ExposedLocalBuffer {

      override def failedRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        failedCounter.get

      override def inflightRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        completionMap.get.map { m => m.count { case (_, completed) => !completed } }

      override def unsentRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        enqueuedCount.get.map(_.toInt)

      override def oldestUnsent: ZIO[Blocking with Clock, LocalBufferError, Option[Long]] =
        (for {
          _ <- readingTailer.moveToActualStart
          success <- readingTailer.index
            .flatMap(index => unsentReportingTailer.moveToIndex(index))
            .tap(moved => Task.when(moved)(unsentReportingTailer.peekDocument))
          timestamp <- if (success) unsentReportingTailer.readCurrentTimestamp else ZIO.none
        } yield timestamp)
          .mapError(LocalBufferError)

      override def close: ZIO[Blocking, LocalBufferError, Unit] = ZIO.unit

      override def enqueue(message: PersistedRecord): ZIO[Clock with Blocking, LocalBufferError, PersistedMessageId] =
        for {
          index <- blocking(RawRecord.writeTo(RawRecord.createFrom(message), appender, writeSem).mapError(LocalBufferError))
          _ <- enqueuedCount.update(_ + 1)
        } yield index

      override def take(upTo: Int): ZIO[Clock with Blocking, LocalBufferError, Seq[PersistedRecord]] =
        (for {
          rawRecords <- readSem.withPermit(ZStream.repeatEffectOption(readingTailer.readOne) // todo possibly read multiple in a managed io
            .run(ZSink.take(upTo)))
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

      override def firstSequenceNumber: ZIO[Blocking, LocalBufferError, Long] = UIO(0L)

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
      .map { case (k, v) => SerDes.encode(k.getBytes("UTF-8")) -> SerDes.encode(v) }
      .map { case (k, v) => s"$k:$v" }
      .mkString(";")
  }

  case class RawRecord(id: ExcerptIndex,
                       topic: String,
                       partition: Partition,
                       key: String,
                       payload: String,
                       headers: String,
                       timestamp: Long) {

    def toPersistedRecord: PersistedRecord =
      PersistedRecord(this.id,
        SerializableTarget(this.topic,
          Option(this.partition).flatMap(p => Some(p).filter(partitionExists)),
          Option(key).map(SerDes.decode)),
        EncodedMessage(Option(payload).map(SerDes.decode),
          SerDes.decodeHeaders(this.headers)))
  }

  object RawRecord {
    val noPartition: Partition = -1

    @inline def partitionExists(partition: Partition): Boolean = {
      !noPartition.equals(partition)
    }

    def createFrom(record: PersistedRecord): RawRecord = {
      RawRecord(record.id,
        record.topic,
        record.target.partition.getOrElse(noPartition),
        record.target.key.map(v => SerDes.encode(v.toArray)).getOrElse(""),
        record.encodedMsg.value.map(v => SerDes.encode(v.toArray)).getOrElse(""),
        encodeHeader(record.encodedMsg.headers),
        System.currentTimeMillis())
    }

    def createFrom(json: String): Either[String, RawRecord] =
      json.fromJson[RawRecord]

    def writeTo(record: RawRecord, appender: ExcerptAppender, semaphore: Semaphore): Task[ExcerptIndex] =
      Task(record.toJson).flatMap(json =>
        semaphore.withPermit(Task(Bytes.from(json))
          .flatMap(bytes => Task(appender.writeBytes(bytes))
            .flatMap(_ => Task(appender.lastIndexAppended())))))

    implicit val decoder: JsonDecoder[RawRecord] =
      DeriveJsonDecoder.gen[RawRecord]

    implicit val encoder: JsonEncoder[RawRecord] =
      DeriveJsonEncoder.gen[RawRecord]
  }

  object SerDes {
    def decodeHeaders(headersString: String): Headers = {
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

}