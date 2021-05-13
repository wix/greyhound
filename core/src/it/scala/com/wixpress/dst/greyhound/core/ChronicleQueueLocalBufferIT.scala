package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.buffers.PersistedMessageId
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.{ChronicleQueueLocalBuffer, EncodedMessage, PersistedRecord, SerializableTarget}
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, TestMetrics, eventuallyZ}
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testenv.ITEnv.Env
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.{Chunk, RManaged, Schedule, UIO, UManaged, ZIO, ZManaged, test}

import scala.util.Random

class ChronicleQueueLocalBufferIT extends BaseTestWithSharedEnv[ITEnv.Env, Any] {
  sequential

  override def sharedEnv: ZManaged[Env, Throwable, Any] = ZManaged.succeed(1)

  override def env: UManaged[ITEnv.Env] =
    for {
      env <- (GreyhoundMetrics.liveLayer ++ test.environment.liveEnvironment).build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  val topic = "topic-x"
  val aRecord = PersistedRecord(0,
    SerializableTarget(topic, Some(555), Some(Chunk.fromArray("key-x".getBytes))),
    EncodedMessage(Some(Chunk.fromArray("payload-x".getBytes())), Headers.from(("header-key-1" -> "header-val-1"), "header-key-2" -> "header-val-2")))

  val aBigRecord = PersistedRecord(200,
    SerializableTarget(topic, Some(555), Some(Chunk.fromArray("key-x".getBytes))),
    EncodedMessage(Some(Chunk.fromArray(("payload-x" * 1024).getBytes())), Headers.from(("header-key-1" -> "header-val-1"), "header-key-2" -> "header-val-2")))

//  "dump" in {
//    net.openhft.chronicle.queue.main.DumpMain.main(Array("/private/tmp/test-producer-75429/20210502F.cq4"))
//    ko("don't run this test automatically - it's here for manual runs")
//  }

/* THIS TEST CREATES A LARGE FILE ON DISK VERY VERY FAST */
//  "cycle" in {
//    val howMany = 10000000
//    val batchSize = 1000
//    val par = 4
//
//    def enqAndTake(buffer: ChronicleQueueLocalBuffer.ExposedLocalBuffer) = {
//      (for {
//        expectedRecords <- UIO.succeed((1 to batchSize).map(i => fakeID(aBigRecord, 100 + i)))
//        _ <- ZIO.foreach(expectedRecords)(buffer.enqueue)
////        records <- buffer.take(batchSize)
////        _ <- ZIO(records.size mustEqual batchSize)
//      } yield ())
//        .repeat(Schedule.recurs(howMany / batchSize / par))
//    }
//
//    queueBuilder("cycle").use { buffer =>
//      ZIO.foreachPar(1 to par)(_ => enqAndTake(buffer)).as(ok)
//    }
//  }

  "enqueue and then take" in {
    val howManyInTotal = 3
    val howManyToTake = 2
    queueBuilder("enqueue").use { buffer =>
      for {
        expectedRecords <- UIO.succeed((1 to howManyInTotal).map(i => fakeID(aRecord, 100 + i)))
        _ <- buffer.unsentRecordsCount.map(_ mustEqual 0)
        _ <- ZIO.foreach(expectedRecords)(buffer.enqueue)
        _ <- buffer.unsentRecordsCount.map(_ mustEqual howManyInTotal /* all records */)
        records <- buffer.take(howManyToTake)
        _ <- ZIO(sameIDs(records) mustEqual sameIDs(records.take(2)))
        _ <- buffer.inflightRecordsCount.map(_ mustEqual howManyToTake /* that have been taken */)
        _ <- buffer.unsentRecordsCount.map(_ mustEqual (howManyInTotal - howManyToTake) /* that have _not_ been taken */)
        _ <- buffer.delete(records.head.id) /* complete 1 */
        _ <- buffer.inflightRecordsCount.map(_ mustEqual (howManyToTake - 1) /* that have been taken but not completed */)
        _ <- buffer.getCompletionMap.get.map(m => m.size.mustEqual(howManyInTotal - howManyToTake /* that still needs to be completed */)).delay(500.milliseconds)
      } yield ok
    }
  }

  "restart a queue with existing messages" in {
    val howMany = 2
    val sameBufferIdentifier = ("restart", Random.nextInt())

    for {

      // enqueue
      _ <- queueBuilder(sameBufferIdentifier).use { buffer =>
        for {
          records <- UIO.succeed(for {
            i <- 1 to howMany
          } yield fakeID(aRecord, i))
          _ <- ZIO.foreach(records)(buffer.enqueue)
          _ <- buffer.unsentRecordsCount.map(_ mustEqual howMany)
        } yield ()
      }

      // restart and take
      _ <- queueBuilder(sameBufferIdentifier).use { buffer =>
        for {
          _ <- buffer.unsentRecordsCount.map(_ mustEqual howMany)
          records <- buffer.take(howMany - 1)
          ids = records.map(_.id)
          _ <- ZIO.foreach(ids)(buffer.delete)
          _ <- eventuallyZ(buffer
            .getCompletionMap.get
            .map(completionMap => ids.map(id => completionMap.getOrElse(id, true)).fold(true)(_ && _)),
            timeout = 2.seconds)(x => x)
        } yield ok
      }

      // restart and check
      _ <- queueBuilder(sameBufferIdentifier).use { buffer =>
        for {
          _ <- buffer.unsentRecordsCount.map(_ mustEqual 1)
        } yield ()
      }

    } yield ok
  }

  "take more than enqueued" in {
    queueBuilder("take-more").use { buffer =>
      for {
        records <- buffer.take(1)
      } yield records mustEqual Seq.empty[PersistedRecord]
    }
  }

  "mark dead" in {
    queueBuilder("mark-dead").use { buffer =>
      for {
        _ <- buffer.markDead(0)
        count <- buffer.failedRecordsCount
      } yield count mustEqual 1
    }
  }

  "oldest unsent" in {
    queueBuilder("oldest-unsent").use { buffer =>
      for {
        aRecord <- ZIO.succeed(fakeID(aRecord, 1000))
        _ <- buffer.enqueue(aRecord)
        _ <- buffer.oldestUnsent.map(_.get must be_>(0L))
        _ <- buffer.take(1)
        _ <- buffer.oldestUnsent.delay(2000.microseconds).map(_ must beNone)
      } yield ok
    }
  }

  def queueBuilder(tuple: (String, Int)):
  RManaged[Clock with Blocking, ChronicleQueueLocalBuffer.ExposedLocalBuffer] =
    queueBuilder(tuple._1, tuple._2)

  def queueBuilder(pathSuffix: String, randomSuffix: Int = Random.nextInt()):
  RManaged[Clock with Blocking, ChronicleQueueLocalBuffer.ExposedLocalBuffer] = {
    ChronicleQueueLocalBuffer.makeInternal(s"/tmp/tests-data/localbuffer-$randomSuffix-$pathSuffix")
  }

  def fakeID(record: PersistedRecord, id: PersistedMessageId) = record.copy(id = id)

  def sameIDs(records: Seq[PersistedRecord]) = records.map(fakeID(_, 666))
}
