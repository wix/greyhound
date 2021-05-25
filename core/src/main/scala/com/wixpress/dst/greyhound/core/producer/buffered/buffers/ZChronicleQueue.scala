package com.wixpress.dst.greyhound.core.producer.buffered.buffers

import com.wixpress.dst.greyhound.core.producer.buffered.buffers.ChronicleQueueLocalBuffer.{ExcerptIndex, RawRecord}
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.ZChronicleQueue.ZTailer
import net.openhft.chronicle.bytes.Bytes
import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptAppender, ExcerptTailer}
import zio.{IO, Task, ZIO}

trait ZChronicleQueue {

  def createTailer(): Task[ZTailer]

  def createTailer(id: String): Task[ZTailer]

  def countExcerpts(from: ExcerptIndex, to: ExcerptIndex): Task[Long]

  def acquireAppender: Task[ExcerptAppender]

  def isClosed: Task[Boolean]
}

object ZChronicleQueue {
  def make(path: String): Task[ZChronicleQueue] = {
    for {
      queue <- Task(ChronicleQueue.singleBuilder(path).build)

    } yield new ZChronicleQueue {

      override def createTailer(): Task[ZTailer] =
        ZTailer.make(queue.createTailer())

      override def createTailer(id: String): Task[ZTailer] =
        ZTailer.make(queue.createTailer(id))

      override def countExcerpts(from: ExcerptIndex, to: ExcerptIndex): Task[Long] =
        Task(queue.countExcerpts(from, to))

      override def acquireAppender: Task[ExcerptAppender] =
        Task(queue.acquireAppender())

      override def isClosed: Task[Boolean] =
        Task.succeed(queue.isClosed)
    }
  }

  trait ZTailer {
    def toEnd: Task[ZTailer]

    def copy: Task[ZTailer]

    def index: Task[ExcerptIndex]

    def moveToIndex(index: ExcerptIndex): Task[Boolean]

    def moveToActualStart: Task[Boolean]

    def peekDocument: Task[Boolean]

    def readCurrentTimestamp: Task[Option[ExcerptIndex]]

    def readOne: IO[Option[Throwable], (ExcerptIndex, RawRecord)]
  }

  object ZTailer {
    def make(queue: ChronicleQueue, id: Option[String]): Task[ZTailer] = id match {
      case Some(i) => make(queue.createTailer(i))
      case _ => make(queue.createTailer())
    }

    def make(tailer: ExcerptTailer): Task[ZTailer] =
      ZIO(new ZTailer {
        override def peekDocument: Task[Boolean] =
          Task(tailer.peekDocument())

        override def toEnd: Task[ZTailer] =
          ZTailer.make(tailer.toEnd)

        override def index: Task[ExcerptIndex] =
          Task(tailer.index())

        override def copy: Task[ZTailer] =
          for {
            result <- make(tailer.queue().createTailer())
            _ <- result.moveToIndex(tailer.index)
          } yield result

        override def moveToIndex(index: ExcerptIndex): Task[Boolean] =
          Task(tailer.moveToIndex(index))

        override def moveToActualStart: Task[Boolean] =
          for {
            hasActualStartIndex <- index.map(_ != 0)
            result <- if (hasActualStartIndex) Task.succeed(false) else peekDocument
          } yield result

        override def readCurrentTimestamp: Task[Option[ExcerptIndex]] =
          readOne
            .map { case (_, record) => Some(record.timestamp) }
            .catchAll(e => e.map(t => ZIO.fail(t)).getOrElse(ZIO.none))

        override def readOne: IO[Option[Throwable], (ExcerptIndex, RawRecord)] =
          for {
            _ <- moveToActualStart.mapError(e => Option(e))
            index <- index.mapError(e => Option(e)) // save index before actual reading
            bytes <- bytes(tailer).mapError(e => Option(e))
            json <- if (bytes.isEmpty) ZIO.fail(None) else ZIO(Bytes.toString(bytes)).mapError(e => Option(e))
            record <- ZIO.fromEither(RawRecord.createFrom(json)).map(r => r.copy(id = index)).mapError(e => Option(new RuntimeException(e)))
          } yield (index, record)

        private def bytes(tailer: ExcerptTailer): Task[Bytes[_]] = {
          ZIO(Bytes.elasticByteBuffer()).flatMap(bytes =>
            Task(tailer.readBytes(bytes)).as(bytes))
        }
      })
  }

}