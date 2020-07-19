package com.wixpress.dst.greyhound.core.producer.buffered.buffers

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
import zio.clock.{Clock, currentTime}
import zio.duration.{Duration, _}
import zio.stream.ZStream

import scala.util.Try

object H2LocalBuffer {
  private val InsertQuery = "INSERT INTO MESSAGES VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"

  def make(localPath: String, keepDeadMessages: Duration): RManaged[Clock, LocalBuffer] =
    (for {
      cp <- Task(JdbcConnectionPool.create(s"jdbc:h2:$localPath;DB_CLOSE_ON_EXIT=FALSE", "greyhound", "greyhound"))
      connection <- Task(cp.getConnection())
      currentSequenceNumber <- Ref.make(0)
      _ <- initDatabase(connection, currentSequenceNumber)(localPath, keepDeadMessages)
    } yield new LocalBuffer {
      override def enqueue(message: PersistedMessage): ZIO[Clock, LocalBufferError, PersistedMessageId] =
        executeInsert(connection, currentSequenceNumber)(message)
          .flatMap(i => when(i < 1)(fail(H2FailedToAppendMessage(message.topic, message.target, i))))
          .mapError(LocalBufferError.apply)
          .as(message.id)

      override def take(upTo: Int): ZIO[Clock, LocalBufferError, Seq[PersistedMessage]] = {
        val takeQuery = s"SELECT * FROM MESSAGES WHERE STATE = '$notSent' ORDER BY SEQ_NUM LIMIT $upTo"

        (for {
          msgs <- query(connection, takeQuery)(rs => list(rs, upTo))
          setPendingQuery = s"UPDATE MESSAGES SET STATE='$pending' WHERE ID IN (${msgs.map(_.id).mkString(",")})"
          _ <- update(connection)(setPendingQuery)
        } yield msgs)
          .mapError(LocalBufferError.apply)
      }

      override def delete(messageId: PersistedMessageId): IO[LocalBufferError, Boolean] =
        update(connection)(s"DELETE TOP 1 FROM MESSAGES WHERE ID=$messageId").map(_ > 0)
          .mapError(LocalBufferError.apply)

      override def markDead(messageId: PersistedMessageId): IO[LocalBufferError, Boolean] =
        update(connection)(s"UPDATE MESSAGES SET STATE='$failed' WHERE ID=$messageId").map(_ > 0)
          .mapError(LocalBufferError.apply)

      override def close: Task[Unit] =
        ZIO.when(!connection.isClosed)(
          Task(connection) *> Task(cp.dispose()))

      override def failedRecordsCount: ZIO[Any, LocalBufferError, Int] =
        query(connection, s"SELECT COUNT(*) FROM MESSAGES WHERE STATE = '$failed'")(rs => Task(rs.next()) *> Task(rs.getInt(1)))
          .mapError(LocalBufferError.apply)


      //      override def numPendingMessages: Int = count(pending)
      //
      //  override def numDeadMessages: Int = count(failed)
      //
      //  override def unsentMessages: Int = count(notSent)
    })
      .toManaged(m => m.close.ignore)

  private def list(resultSet: ResultSet, count: Int): Task[List[PersistedMessage]] = {
    def next(rs: ResultSet): IO[Option[Throwable], PersistedMessage] = {
      (for {
        _ <- ZIO.when(!rs.next)(ZIO.fail(null))
        produceKey <- Task(Try(Chunk.fromArray(rs.getBytes(3))).toOption)
        topic = rs.getString(2)
        producePartition <- Task(rs.getInt(4)).map(p => if (p >= 0) Option(p) else None)
        id <- Task(rs.getLong(1))
        encodedPayload = Try(Chunk.fromArray(rs.getBytes(5))).toOption.orNull
        header <- decodeHeaders(rs.getString(6))
        submitted <- UIO(rs.getLong(9))
      } yield PersistedMessage(id, SerializableTarget(topic, producePartition, produceKey), EncodedMessage(encodedPayload, header), submitted))
        .mapError(Option(_))
    }

    ZStream.repeatEffectOption(next(resultSet))
      //creates a stream that ends when !rs.next
      .take(count)
      .runCollect
      .map(_.toList)
  }


  private def executeInsert(connection: Connection, currentSequenceNumber: Ref[Int])(message: PersistedMessage): RIO[Clock, Int] =
    for {
      insertStatement <- Task(connection.prepareStatement(InsertQuery))
      payloadBytes = Option(message.encodedMsg.value).map(_.toArray).orNull
      base64Headers <- encodeHeaderToBase64(message)
      base64Key <- keyBytes(message)
      lastSeqNum <- currentSequenceNumber.updateAndGet(_ + 1)
      timeMillis <- clock.currentTime(MILLISECONDS)
      _ <- Task {
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
      res <- Task(insertStatement.executeUpdate())
    } yield res


  private def keyBytes(message: PersistedMessage): Task[Option[Array[Byte]]] =
    Task(message.target.key.map(_.toArray))

  private def encodeHeaderToBase64(message: PersistedMessage): ZIO[Any, Throwable, String] = {
    ZIO.foreach(message.encodedMsg.headers.headers) { case (k, v) => Base64Adapter.encode(k.getBytes("UTF-8")) zip Base64Adapter.encode(v) }
      .map(pairs => pairs.map { case (k, v) => s"$k:$v" })
      .map(_.mkString(";"))
  }

  private def decodeHeaders(headersString: String): ZIO[Any, Throwable, Headers] = {
    ZIO.foreach(headersString.split(';').filter(_.nonEmpty).map(part => {
      val key :: value :: Nil = part.split(":").toList
      (key, value)
    })) { case (base64Key, base64Value) =>
      Base64Adapter.decode(base64Key).map(b => new String(b.toArray, "UTF-8")) zip Base64Adapter.decode(base64Value)
    }.map(headers => Headers(headers.toMap))
  }


  private def initDatabase(connection: Connection, currentSequenceNumber: Ref[Int])(localPath: String, keepDeadMessages: Duration): RIO[Clock, Unit] =
    withStatement(connection)(statement =>
      createTableIfNeed(statement)(localPath) *>
        restoreUnsentStatusForPendingMessages(statement) *>
        setLastSequenceNumber(statement, currentSequenceNumber) *>
        deleteObsoleteDeadMessages(statement)(keepDeadMessages))

  private def createTableIfNeed(statement: Statement)(localPath: String): Task[Unit] =
    Task(Files.createDirectory(Paths.get(localPath.substring(0, localPath.lastIndexOf("/"))))).catchSome { case _: FileAlreadyExistsException => ZIO.unit } *>
      ZIO.foreach(
        Seq(
          "CREATE TABLE IF NOT EXISTS MESSAGES(ID BIGINT, TOPIC VARCHAR, KEY BINARY, PARTITION INT, MESSAGE BINARY, HEADERS CLOB, STATE VARCHAR, SEQ_NUM BIGINT, SUBMITTED BIGINT)",
          "CREATE INDEX IF NOT EXISTS SEQNUM_INDEX ON MESSAGES(SEQ_NUM)",
          "CREATE INDEX IF NOT EXISTS STATE_INDEX ON MESSAGES(STATE)",
          "CREATE INDEX IF NOT EXISTS ID_INDEX ON MESSAGES(ID)"))(
        line => Task(statement.execute(line)))
        .unit

  private def setLastSequenceNumber(statement: Statement, currentSequenceNumber: Ref[Int]): Task[Unit] =
    Task(statement.executeQuery("SELECT MAX(SEQ_NUM) FROM MESSAGES"))
      .flatMap(rs => when(rs.next())(currentSequenceNumber.set(rs.getInt(1))))

  private def restoreUnsentStatusForPendingMessages(statement: Statement): Task[Boolean] =
    Task(statement.execute(s"UPDATE MESSAGES SET STATE='$notSent' WHERE STATE != '$failed'"))

  private def deleteObsoleteDeadMessages(statement: Statement)(keepDeadMessages: Duration): RIO[Clock, Unit] =
    currentTime(MILLISECONDS)
      .map(_ - keepDeadMessages.toMillis)
      .flatMap(purgeFrom =>
        Task(statement.execute(s"DELETE FROM MESSAGES WHERE STATE = '$failed' AND SUBMITTED < $purgeFrom")))
}


object Base64Adapter {
  private val encoder = Base64.getEncoder
  private val decoder = Base64.getDecoder

  def encode(bytes: Chunk[Byte]): Task[String] =
    encode(bytes.toArray)

  def encode(bytes: Array[Byte]): Task[String] = Task {
    Option(bytes) match {
      case None => null
      case _ => new String(encoder.encode(bytes), "UTF-8")
    }
  }

  def decode(message: String): Task[Chunk[Byte]] = Task {
    Option(message) match {
      case None => Chunk.empty
      case _ => Chunk.fromArray(decoder.decode(message.getBytes("UTF-8")))
    }
  }
}


object H2StatementSupport {
  val notSent = "NOT_SENT"
  val failed = "FAILED"
  val pending = "PENDING"
  val defaultKeepDeadMessages: Duration = 7.days

  def query[K](connection: Connection, queryStr: String)(f: ResultSet => Task[K]): Task[K] =
    withStatement(connection) { statement =>
      ZManaged.make(
        acquire = Task(statement.executeQuery(queryStr)))(
        release = rs => Task(rs.close()).catchAll(e => UIO(e.printStackTrace())))
        .use(f)
    }

  def update(connection: Connection)(updateQuery: String): Task[Int] =
    withStatement(connection)(statement => Task(statement.executeUpdate(updateQuery)))

  def withStatement[R, K](connection: Connection)(f: Statement => RIO[R, K]): RIO[R, K] =
    ZManaged.make(
      acquire = Task(connection.createStatement()))(
      release = statement => Task(statement.close()).unit.catchAll(e => UIO(e.printStackTrace())))
      .use(f)
}

case class H2FailedToAppendMessage(topic: Topic, target: SerializableTarget, result: Int) extends RuntimeException(s"H2 Local DB returned $result ( != 1 )")


//
//class H2MessagesQueue(path: String,
//                      keepDeadMessages: Duration = defaultKeepDeadMessages,
//                      clock: TimeProvider = new SystemTimeProvider) {
//  lazy private val cp = JdbcConnectionPool.create(s"jdbc:h2:$path;DB_CLOSE_ON_EXIT=FALSE", "greyhound", "greyhound")
//  override lazy protected val connection = cp.getConnection()
//  private val currentSequenceNumber = new AtomicLong(0)
//  //  private val base64 = new Base64Adapter
//
//  init()
//
//  private def init(): Unit = withStatement { statement =>
//    createTableIfNeed(statement)
//    restoreUnsentStatusForPendingMessages(statement)
//    setLastSequenceNumber(statement)
//    deleteObsoleteDeadMessages(statement)
//  }
//
//  private def setLastSequenceNumber(statement: Statement) = {
//    val resultSet = statement.executeQuery("SELECT MAX(SEQ_NUM) FROM MESSAGES")
//    if (resultSet.next())
//      currentSequenceNumber.set(resultSet.getInt(1))
//  }
//
//  private def restoreUnsentStatusForPendingMessages(statement: Statement) = {
//    statement.execute(s"UPDATE MESSAGES SET STATE='$notSent' WHERE STATE != '$failed'")
//  }
//
//  private def deleteObsoleteDeadMessages(statement: Statement) = {
//    val purgeFrom = clock.time - keepDeadMessages.toMillis
//    statement.execute(s"DELETE FROM MESSAGES WHERE STATE = '$failed' AND SUBMITTED < $purgeFrom")
//  }
//
//  private def createTableIfNeed(statement: Statement) = {
//    Try(Files.createDirectory(Paths.get(path.substring(0, path.lastIndexOf("/")))))
//    Seq(
//      "CREATE TABLE IF NOT EXISTS MESSAGES(ID BIGINT, TARGET VARCHAR, MESSAGE CLOB, HEADERS CLOB, STATE VARCHAR, SEQ_NUM BIGINT, SUBMITTED BIGINT)",
//      "CREATE INDEX IF NOT EXISTS SEQNUM_INDEX ON MESSAGES(SEQ_NUM)",
//      "CREATE INDEX IF NOT EXISTS STATE_INDEX ON MESSAGES(STATE)",
//      "CREATE INDEX IF NOT EXISTS ID_INDEX ON MESSAGES(ID)")
//      .foreach(statement.execute)
//  }
//
//  private def executeInsert(message: PersistedMessage): Int = {
//    val insertStatement = connection.prepareStatement(insertQuery)
//
//    val base64Payload = base64.encode(message.encodedMsg.encodedPayload)
//    val base64Headers = base64.encode(Option(message.encodedMsg.header).getOrElse(Header()).asJsonStr)
//    val base64ProduceTarget = base64.encode(message.target.asJsonStr)
//
//    insertStatement.setLong(1, message.id)
//    insertStatement.setString(2, base64ProduceTarget)
//    insertStatement.setString(3, base64Payload)
//    insertStatement.setString(4, base64Headers)
//    insertStatement.setString(5, notSent)
//    insertStatement.setLong(6, currentSequenceNumber.incrementAndGet)
//    insertStatement.setLong(7, clock.time)
//
//    insertStatement.executeUpdate()
//  }
//
//  override def enqueue(message: PersistedMessage): Int = {
//    executeInsert(message)
//  }
//
//  override def take(batchSize: Int): Seq[PersistedMessage] = {
//    val takeQuery = s"SELECT * FROM MESSAGES WHERE STATE = '$notSent' ORDER BY SEQ_NUM LIMIT $batchSize"
//    val msgs = query(takeQuery)(rs => list(rs, batchSize))
//
//    val setPendingQuery = s"UPDATE MESSAGES SET STATE='$pending' WHERE ID IN (${msgs.map(_.id).mkString(",")})"
//    update(setPendingQuery)
//
//    msgs
//  }
//
//  override def delete(messageId: PersistedMessageId): Boolean =
//    update(s"DELETE TOP 1 FROM MESSAGES WHERE ID=$messageId") > 0
//
//  override def shutdown(): Unit = {
//    if (!connection.isClosed) {
//      connection.close()
//      cp.dispose()
//    }
//  }
//
//  override def markDead(messageId: PersistedMessageId): Boolean =
//    update(s"UPDATE MESSAGES SET STATE='$failed' WHERE ID=$messageId") > 0
//
//  def purgeAll() = update("DELETE FROM MESSAGES")
//
//  private def failedMessagesCondition(from: Int, to: Int) =
//    s"WHERE STATE = '$failed' ORDER BY SEQ_NUM OFFSET $from LIMIT ${to - from + 1}"
//
//  override def failedMessages(from: Int, to: Int): Seq[PersistedMessage] = {
//    query(s"SELECT * FROM MESSAGES ${failedMessagesCondition(from, to)}") {
//      rs => list(rs, to - from + 1)
//    }
//  }
//
//
//  override def deleteFailedMessages(from: Int, to: Int): Int =
//    update(s"DELETE FROM MESSAGES WHERE ID IN (SELECT ID FROM MESSAGES ${failedMessagesCondition(from, to)})")
//
//  override def numPendingMessages: Int = count(pending)
//
//  override def numDeadMessages: Int = count(failed)
//
//  override def unsentMessages: Int = count(notSent)
//
//  private def count(field: String) = query(s"SELECT COUNT(*) FROM MESSAGES WHERE STATE = '$field'") {
//    rs =>
//      rs.next()
//      rs.getInt(1)
//  }
//
//  override def oldestUnsent =
//    query(s"SELECT SUBMITTED FROM MESSAGES WHERE STATE = '$notSent' ORDER BY SEQ_NUM LIMIT 1") {
//      rs => (if (rs.next()) clock.time - rs.getLong("SUBMITTED") else 0l).millis
//    }
//
//  private def list(resultSet: ResultSet, count: Int) =
//    new Iterator[PersistedMessage] {
//      def hasNext = resultSet.next()
//
//      def next() = {
//        val produceTarget = base64.decode(resultSet.getString(2)).as[ProduceTarget]
//        val id = resultSet.getLong(1)
//        val encodedPayload = base64.decode(resultSet.getString(3))
//        val header = base64.decode(resultSet.getString(4)).as[Header]
//        val submitted = resultSet.getLong(7)
//        PersistedMessage(id, produceTarget, EncodedMessage(encodedPayload, header), submitted)
//      }
//    }.take(count).toList
//}
