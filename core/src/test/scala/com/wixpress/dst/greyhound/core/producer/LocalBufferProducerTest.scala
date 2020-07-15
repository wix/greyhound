package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core.Offset
import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.producer.MockProducer.{Key, Value}
import com.wixpress.dst.greyhound.core.producer.buffered.buffers._
import com.wixpress.dst.greyhound.core.producer.buffered.{BufferedProduceResult, LocalBufferProducer}
import com.wixpress.dst.greyhound.core.testkit.TestClockUtils.adjustClock
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, TestMetrics, eventuallyTimeout}
import org.apache.kafka.common.errors.{RecordTooLargeException, TimeoutException}
import org.apache.kafka.common.serialization.Serdes.{IntegerSerde, StringSerde => KStringSerde}
import org.specs2.specification.Scope
import zio.Schedule.recurs
import zio.ZIO.sleep
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.util.Random

class LocalBufferProducerTest extends BaseTest[ZEnv] {
  sequential

  override def env: UManaged[ZEnv] = testEnv

  def testEnv: UManaged[ZEnv] =
    for {
      env <- zio.test.environment.testEnvironment.build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  "produce in order of per key" in new Context {
    for {
      producer <- MockProducer.make.tap(_.stopFailing)
      maxConcurrency = 10
      localBufferProducer <- makeProducer(producer, _.withMaxConcurrency(maxConcurrency))
      record = ProducerRecord(topic, "0", Some(0))
      (keyCount, recordPerKey) = (100, 20)
      _ <- produceMultiple(keyCount, recordPerKey)(localBufferProducer, record) *>
        eventuallyTimeout(producer.produced)(_ == expectedMap(recordPerKey, keyCount))(20.seconds)
      activeFibers <- localBufferProducer.currentState.map(_.maxRecordedConcurrency)
    } yield activeFibers === maxConcurrency
  }

  "keep retrying on retriable errors" in new Context {
    val (key, exception) = (0, new TimeoutException)
    for {
      producer <- MockProducer.make.tap(_.startFailing(exception))
      localBufferProducer <- makeProducer(producer)
      record = ProducerRecord[Key, String](topic, "0", key = Some(key))
      producedFiber <- localBufferProducer.produce(record, IntSerde, StringSerde).flatMap(_.kafkaResult)
        .timeout(100.seconds) // this only timeouts waiting, it's still going to continue retrying
        .fork
      _ <- adjustClock(100.seconds)
      produced <- producedFiber.join
      failuresForKey <- producer.failures.map(_.getOrElse(key, Nil))
    } yield (produced === None) and (failuresForKey.map(_.getClass.getName) === (0 to 100).map(_ => "com.wixpress.dst.greyhound.core.producer.TimeoutError"))
  }

  "not retry on unretriable errors" in new Context {
    val (key, exception) = (0, new RecordTooLargeException)
    val record = ProducerRecord[Key, String](topic, "0", key = Some(key))

    for {
      producer <- MockProducer.make.tap(_.startFailing(exception))
      localBufferProducer <- makeProducer(producer)
      producedError <- localBufferProducer.produce(record, IntSerde, StringSerde).flatMap(_.kafkaResult.flip)
      failuresForKey <- producer.failures.map(_.getOrElse(key, Nil))
    } yield
      (producedError.getCause.getClass === classOf[RecordTooLargeException]) and
        (failuresForKey.size === 1)
  }

  "throw exceptions when persistent buffer gets filled" in new Context {
    val key = 0
    for {
      producer <- MockProducer.make.tap(_.setDelay(10.seconds))
      localBufferProducer <- makeProducer(producer, _.withMaxMessagesOnDisk(999))
      record = ProducerRecord[Key, String](topic, "0", key = Some(key))
      produceIO = localBufferProducer.produce(record, IntSerde, StringSerde)
      _ <- produceIO.repeat(recurs(999))
      _ <- zio.console.putStrLn("after 999")
      error <- produceIO.flip // 999 + 1
      _ <- zio.console.putStrLn("after 1000")
    } yield error === LocalBufferError(LocalBufferFull(999))
  }

  "not try to send messages if their submit time is older than configured timeout" in new Context {
    val key = 0
    for {
      producer <- MockProducer.make.tap(_.startFailing(new TimeoutException))
      localBufferProducer <- makeProducer(producer, _.withGiveUpAfter(5.millis))
      record = ProducerRecord[Key, String](topic, "0", key = Some(key))
      produceFiber <- localBufferProducer.produce(record, IntSerde, StringSerde).flatMap(_.kafkaResult.flip).fork
      _ <- adjustClock(100.millis)
      producerError <- produceFiber.join
    } yield producerError.getClass === classOf[TimeoutError]
  }
  
  class Context extends Scope {
    val topic = s"topic-${Random.nextInt(50000)}"
  }

  // todo next up:
  //  "flush all pending records"
  //  "expose current state (including activeness of buffer for health tests)"
  //  wix-layer:
  //   state gauges
  //   health test
  //   shutdown wait 30 seconds for flush

  private def produceMultiple(keyCount: Int, recordPerKey: Int)(localBufferProducer: LocalBufferProducer, record: ProducerRecord[Key, Value]): ZIO[zio.ZEnv, LocalBufferError, List[BufferedProduceResult]] =
    ZIO.foreach(0 until (keyCount * recordPerKey)) { i =>
      localBufferProducer.produce(record.copy(value = i.toString, key = Some(i % keyCount)), IntSerde, StringSerde)
    }

  private def expectedMap(recordPerKey: Int, keyCount: Int): Map[Key, Seq[Value]] =
    (0 until keyCount).map(key => key -> expectedListForKey(key, recordPerKey, keyCount)).toMap

  private def expectedListForKey(key: Key, recordPerKey: Int, keyCount: Int): Seq[Value] =
    (0 until recordPerKey).map(i => keyCount * i + key).map(_.toString)

  private def makeProducer(producer: Producer,
                           configChange: LocalBufferProducerConfig => LocalBufferProducerConfig = identity) =
    makeH2Buffer.flatMap(buffer =>
      LocalBufferProducer.make(producer, buffer, configChange(LocalBufferProducerConfig(maxConcurrency = 10, maxMessagesOnDisk = 10000, giveUpAfter = 1.day))))

  private def makeH2Buffer: RIO[Clock, LocalBuffer] =
    H2LocalBuffer.make(s"./tests-data/test-producer-${Math.abs(Random.nextInt(100000))}", keepDeadMessages = 1.day)
}

object TimeoutProducingRecord extends RuntimeException

trait MockProducer extends Producer {
  def produced: UIO[Map[Key, Seq[Value]]]

  def failures: UIO[Map[Key, Seq[ProducerError]]]

  def startFailing(failure: Exception): UIO[Unit]

  def stopFailing: UIO[Unit]

  def setDelay(delay: Duration): UIO[Unit]
}

object MockProducer {
  type Key = Int
  type Value = String

  def make: URIO[Clock, MockProducer] = for {
    ref <- Ref.make(Map.empty[Key, Seq[Value]])
    failuresRef <- Ref.make(Map.empty[Key, Seq[ProducerError]])
    delayRef <- Ref.make(0.millis)
    shouldFailProducesRef <- Ref.make(Option[ProducerError](null))
    clock <- ZIO.environment[Clock]
    producer = new Producer {
      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, ZIO[Any, ProducerError, RecordMetadata]] =
        maybeDelay(delayRef, clock) *>
          shouldFailProducesRef.get.map {
            case Some(error) =>
              saveFailure(failuresRef)(keyFrom(record), error)
            case None => saveOffset(ref)(keyFrom(record), valueFrom(record))
              .map(offset => RecordMetadata(record.topic, partition = 0, offset = offset))
          }


    }
  } yield new MockProducer {

    override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, ZIO[Any, ProducerError, RecordMetadata]] =
      producer.produceAsync(record)

    override def produced: UIO[Map[Key, Seq[Value]]] =
      ref.get

    override def failures: UIO[Map[Key, Seq[ProducerError]]] =
      failuresRef.get

    private def returning(result: Either[ProducerError, Unit]): UIO[Unit] =
      shouldFailProducesRef.set(result.swap.toOption)

    override def startFailing(failure: Exception): UIO[Unit] = ProducerError(failure).flip.flatMap(error => returning(Left(error)))

    override def stopFailing: UIO[Unit] = returning(Right(Unit))

    override def setDelay(delay: Duration): UIO[Unit] = delayRef.set(delay)
  }

  private def maybeDelay(delayRef: Ref[Duration], clock: Clock) =
    delayRef.get.flatMap(d => ZIO.when(d > 0.millis)(sleep(d))).provide(clock)

  private def saveOffset(consumed: Ref[Map[Key, Seq[Value]]]): (Key, Value) => UIO[Offset] =
    (key, value) => consumed.updateAndGet(map => map + (key -> (map.getOrElse(key, Nil) :+ value)))
      .map(_.getOrElse(key, Nil).size - 1)

  private def saveFailure(failures: Ref[Map[Key, Seq[ProducerError]]]): (Key, ProducerError) => IO[ProducerError, RecordMetadata] =
    (key, failure) => failures.updateAndGet(map => map + (key -> (map.getOrElse(key, Nil) :+ failure)))
      .as(failure).flip

  private def valueFrom(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) =
    new KStringSerde().deserializer().deserialize(record.topic, record.value.toArray)

  private def keyFrom(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) =
    new IntegerSerde().deserializer.deserialize(record.topic, record.key.get.toArray).toInt
}