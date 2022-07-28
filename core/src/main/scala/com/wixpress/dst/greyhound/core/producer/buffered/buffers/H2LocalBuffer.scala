package com.wixpress.dst.greyhound.core.producer.buffered.buffers

import java.io.File
import java.nio.file.{FileAlreadyExistsException, Files, Paths}
import java.sql.{Connection, ResultSet, Statement}
import java.util.Base64
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core.{Headers, Topic}
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.H2StatementSupport.{withStatement, _}
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import org.h2.jdbcx.JdbcConnectionPool
import zio.ZIO.{fail, when}
import zio._
import zio.blocking.{effectBlocking, Blocking}
import zio.clock.{currentTime, Clock}
import zio.duration.{Duration, _}
import zio.stream.ZStream

import scala.util.Try

object H2LocalBuffer {
  private val InsertQuery = "INSERT INTO MESSAGES VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"

  def make(localPath: String, keepDeadMessages: Duration, startFrom: Option[Long] = None): RManaged[Clock with Blocking, LocalBuffer] =
    (for {
      cp                    <- effectBlocking(JdbcConnectionPool.create(s"jdbc:h2:$localPath;DB_CLOSE_ON_EXIT=FALSE", "greyhound", "greyhound"))
      connection            <- effectBlocking(cp.getConnection())
      currentSequenceNumber <- Ref.make(0L)
      closedRef             <- Ref.make(false)
      _                     <- UIO(println("creating new h2 buffer!"))
      _                     <- initDatabase(connection, currentSequenceNumber)(localPath, keepDeadMessages, startFrom)
    } yield new LocalBuffer {
      override def enqueue(message: PersistedRecord): ZIO[Clock with Blocking, LocalBufferError, PersistedMessageId] =
        executeInsert(connection, currentSequenceNumber)(message)
          .flatMap(i => when(i < 1)(fail(H2FailedToAppendMessage(message.topic, message.target, i))))
          .mapError(LocalBufferError.apply)
          .as(message.id)

      override def take(upTo: Int): ZIO[Clock with Blocking, LocalBufferError, Seq[PersistedRecord]] = {
        val takeQuery = s"SELECT * FROM MESSAGES WHERE STATE = '$notSent' ORDER BY SEQ_NUM LIMIT $upTo"

        (for {
          msgs           <- query(connection)(takeQuery)(rs => list(rs, upTo))
          setPendingQuery = s"UPDATE MESSAGES SET STATE='$pending' WHERE ID IN (${msgs.map(_.id).mkString(",")})"
          _              <- update(connection)(setPendingQuery)
        } yield msgs)
          .mapError(LocalBufferError.apply)
      }

      // wait, current sequence number just shows the last appended.
      // it doesn't say the last written
      override def lastSequenceNumber: UIO[Long] = currentSequenceNumber.get

      override def firstSequenceNumber: ZIO[Blocking, LocalBufferError, Long] =
        query(connection)("SELECT MIN(SEQ_NUM) FROM MESSAGES")(rs => Task(rs.next()) *> Task(rs.getLong(1)))
          .mapError(LocalBufferError.apply)

      override def delete(messageId: PersistedMessageId): ZIO[Blocking, LocalBufferError, Boolean] =
        update(connection)(s"DELETE TOP 1 FROM MESSAGES WHERE ID=$messageId")
          .map(_ > 0)
          .mapError(LocalBufferError.apply)

      override def markDead(messageId: PersistedMessageId): ZIO[Blocking, LocalBufferError, Boolean] =
        update(connection)(s"UPDATE MESSAGES SET STATE='$failed' WHERE ID=$messageId")
          .map(_ > 0)
          .mapError(LocalBufferError.apply)

      override def close: ZIO[Blocking, LocalBufferError, Unit] =
        closedRef.set(true) *>
          ZIO
            .when(!connection.isClosed)(effectBlocking(connection.close()) *> effectBlocking(cp.dispose()))
            .mapError(LocalBufferError.apply)

      override def failedRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        count(connection)(failed)

      override def inflightRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        count(connection)(pending)

      override def unsentRecordsCount: ZIO[Blocking, LocalBufferError, Int] =
        count(connection)(notSent)

      override def oldestUnsent: ZIO[Blocking with Clock, LocalBufferError, Option[Long]] =
        query(connection)(s"SELECT SUBMITTED FROM MESSAGES WHERE STATE = '$notSent' ORDER BY SEQ_NUM LIMIT 1") { rs =>
          Task(rs.next()).map(found => if (found) Some(rs.getLong("SUBMITTED")) else None)
        }
          .mapError(LocalBufferError.apply)

      override def cleanup: ZIO[Blocking, LocalBufferError, Unit] =
        (effectBlocking(connection.close()) *> effectBlocking(new File(s"$localPath.mv.db").delete()))
          .catchAll(e => ZIO.fail(LocalBufferError(e)))
          .unit

      override def isOpen: UIO[Boolean] = closedRef.get.map(!_)
    })
      .toManaged(m => m.close.ignore)

  private def count(connection: Connection)(field: String) =
    query(connection)(s"SELECT COUNT(*) FROM MESSAGES WHERE STATE = '$field'")(rs => Task(rs.next()) *> Task(rs.getInt(1)))
      .mapError(LocalBufferError.apply)

  private def list(resultSet: ResultSet, count: Int): Task[List[PersistedRecord]] = {
    def next(rs: ResultSet): IO[Option[Throwable], PersistedRecord] = {
      (for {
        _                <- ZIO.when(!rs.next)(ZIO.fail(null))
        produceKey       <- Task(Try(Chunk.fromArray(rs.getBytes(3))).toOption)
        topic             = rs.getString(2)
        producePartition <- Task(rs.getInt(4)).map(p => if (p != -1) Option(p) else None)
        id               <- Task(rs.getLong(1))
        encodedPayload    = Try(Chunk.fromArray(rs.getBytes(5))).toOption
        header           <- decodeHeaders(rs.getString(6))
        submitted        <- UIO(rs.getLong(9))
      } yield PersistedRecord(
        id,
        SerializableTarget(topic, producePartition, produceKey),
        EncodedMessage(encodedPayload, header),
        submitted
      ))
        .mapError(Option(_))
    }

    ZStream
      .repeatEffectOption(next(resultSet))
      // creates a stream that ends when !rs.next
      .take(count)
      .runCollect
      .map(_.toList)
  }

  private def executeInsert(connection: Connection, currentSequenceNumber: Ref[Long])(
    message: PersistedRecord
  ): RIO[Clock with Blocking, Int] =
    for {
      insertStatement <- effectBlocking(connection.prepareStatement(InsertQuery))
      payloadBytes     = message.encodedMsg.value.map(_.toArray).orNull
      base64Headers   <- encodeHeaderToBase64(message)
      base64Key       <- keyBytes(message)
      lastSeqNum      <- currentSequenceNumber.updateAndGet(_ + 1)
      timeMillis      <- clock.currentTime(MILLISECONDS)
      _               <- Task {
                           insertStatement.setLong(1, message.id)
                           insertStatement.setString(2, message.topic)
                           insertStatement.setBytes(3, base64Key.orNull)
                           insertStatement.setInt(4, message.target.partition.getOrElse(-1))
                           insertStatement.setBytes(5, payloadBytes)
                           insertStatement.setString(6, base64Headers)
                           insertStatement.setString(7, notSent)
                           insertStatement.setLong(8, lastSeqNum)
                           insertStatement.setLong(9, timeMillis)
                         }
      res             <- effectBlocking(insertStatement.executeUpdate())
    } yield res

  private def keyBytes(message: PersistedRecord): Task[Option[Array[Byte]]] =
    Task(message.target.key.map(_.toArray))

  private def encodeHeaderToBase64(message: PersistedRecord): ZIO[Any, Throwable, String] = {
    ZIO
      .foreach(message.encodedMsg.headers.headers) { case (k, v) => Base64Adapter.encode(k.getBytes("UTF-8")) zip Base64Adapter.encode(v) }
      .map(pairs => pairs.map { case (k, v) => s"$k:$v" })
      .map(_.mkString(";"))
  }

  private def decodeHeaders(headersString: String): ZIO[Any, Throwable, Headers] = {
    ZIO
      .foreach(
        headersString
          .split(';')
          .filter(_.nonEmpty)
          .filter(_.split(":").length == 2)
          .map(part => {
            val key :: value :: Nil = part.split(":").toList
            (key, value)
          })
          .toSeq
      ) {
        case (base64Key, base64Value) =>
          Base64Adapter.decode(base64Key).map(b => new String(b.toArray, "UTF-8")) zip Base64Adapter.decode(base64Value)
      }
      .map(headers => Headers(headers.toMap))
  }

  private def deleteUpTo(connection: Connection)(seqNum: Long) =
    update(connection)(s"DELETE FROM MESSAGES WHERE SEQ_NUM < $seqNum")
      .tap(i => UIO(println(s"deleted ${i} records (seqNum to delete before: ${seqNum})!")))
      .mapError(LocalBufferError.apply)

  private def initDatabase(
    connection: Connection,
    currentSequenceNumber: Ref[Long]
  )(localPath: String, keepDeadMessages: Duration, startFrom: Option[Long]): RIO[Clock with Blocking, Unit] =
    withStatement(connection)(statement =>
      createTableIfNeed(statement)(localPath) *> restoreUnsentStatusForPendingMessages(statement) *>
        setLastSequenceNumber(statement, currentSequenceNumber) *> deleteObsoleteDeadMessages(statement)(keepDeadMessages) *>
        ZIO.when(startFrom.isDefined)(deleteUpTo(connection)(startFrom.get))
    )

  private def createTableIfNeed(statement: Statement)(localPath: String): RIO[Blocking, Unit] =
    effectBlocking(Files.createDirectory(Paths.get(localPath.substring(0, localPath.lastIndexOf("/"))))).catchSome {
      case _: FileAlreadyExistsException => ZIO.unit
    } *>
      ZIO
        .foreach(
          Seq(
            "CREATE TABLE IF NOT EXISTS MESSAGES(ID BIGINT, TOPIC VARCHAR, KEY BINARY, PARTITION INT, MESSAGE BINARY, HEADERS CLOB, STATE VARCHAR, SEQ_NUM BIGINT, SUBMITTED BIGINT)",
            "CREATE INDEX IF NOT EXISTS SEQNUM_INDEX ON MESSAGES(SEQ_NUM)",
            "CREATE INDEX IF NOT EXISTS STATE_INDEX ON MESSAGES(STATE)",
            "CREATE INDEX IF NOT EXISTS ID_INDEX ON MESSAGES(ID)"
          )
        )(line => effectBlocking(statement.execute(line)))
        .unit

  private def setLastSequenceNumber(statement: Statement, currentSequenceNumber: Ref[Long]): RIO[Blocking, Unit] =
    effectBlocking(statement.executeQuery("SELECT MAX(SEQ_NUM) FROM MESSAGES"))
      .flatMap(rs => when(rs.next())(currentSequenceNumber.set(rs.getLong(1))))

  private def restoreUnsentStatusForPendingMessages(statement: Statement): RIO[Blocking, Boolean] =
    effectBlocking(statement.execute(s"UPDATE MESSAGES SET STATE='$notSent' WHERE STATE != '$failed'"))

  private def deleteObsoleteDeadMessages(statement: Statement)(keepDeadMessages: Duration): RIO[Clock with Blocking, Unit] =
    currentTime(MILLISECONDS)
      .map(_ - keepDeadMessages.toMillis)
      .flatMap(purgeFrom => effectBlocking(statement.execute(s"DELETE FROM MESSAGES WHERE STATE = '$failed' AND SUBMITTED < $purgeFrom")))
}

object Base64Adapter {
  private val encoder = Base64.getEncoder
  private val decoder = Base64.getDecoder

  def encode(bytes: Chunk[Byte]): Task[String] =
    encode(bytes.toArray)

  def encode(bytes: Array[Byte]): Task[String] = Task {
    Option(bytes) match {
      case None => null
      case _    => new String(encoder.encode(bytes), "UTF-8")
    }
  }

  def decode(message: String): Task[Chunk[Byte]] = Task {
    Option(message) match {
      case None => Chunk.empty
      case _    => Chunk.fromArray(decoder.decode(message.getBytes("UTF-8")))
    }
  }
}

object H2StatementSupport {
  val notSent                           = "NOT_SENT"
  val failed                            = "FAILED"
  val pending                           = "PENDING"
  val defaultKeepDeadMessages: Duration = 7.days

  def query[K](connection: Connection)(queryStr: String)(f: ResultSet => Task[K]): RIO[Blocking, K] =
    withStatement(connection) { statement =>
      ZManaged
        .make(acquire = effectBlocking(statement.executeQuery(queryStr)))(
          release = rs => effectBlocking(rs.close()).catchAll(e => UIO(e.printStackTrace()))
        )
        .use(f)
    }

  def update(connection: Connection)(updateQuery: String): RIO[Blocking, Int] =
    withStatement(connection)(statement => effectBlocking(statement.executeUpdate(updateQuery)))

  def withStatement[R, K](connection: Connection)(f: Statement => RIO[R with Blocking, K]): RIO[R with Blocking, K] =
    ZManaged
      .make(acquire = effectBlocking(connection.createStatement()))(
        release = statement => effectBlocking(statement.close()).unit.catchAll(e => UIO(e.printStackTrace()))
      )
      .use(f)
}

case class H2FailedToAppendMessage(topic: Topic, target: SerializableTarget, result: Int)
    extends RuntimeException(s"H2 Local DB returned $result ( != 1 )")
