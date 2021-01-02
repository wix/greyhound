package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.producer.Producer.Producer
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import com.wixpress.dst.greyhound.core.producer.ReportingProducerTest._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, FakeProducer, TestMetrics}
import zio._
import zio.blocking.Blocking
import zio.duration._
import zio.test.environment.{TestClock, TestEnvironment}

import scala.concurrent.duration.FiniteDuration

class ReportingProducerTest extends BaseTest[TestEnvironment with TestMetrics] {
  override def env: UManaged[TestEnvironment with TestMetrics] = testEnv

  def testEnv: UManaged[TestEnvironment with TestMetrics] =
    for {
      env <- zio.test.environment.testEnvironment.build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  "delegate to internal producer" in {
    for {
      internal <- FakeProducer.make
      producer = ReportingProducer(internal)
      _ <- producer.produce(record)
      produced <- internal.records.take
    } yield produced must equalTo(record)
  }

  "report metric when producing" in {
    for {
      fakeProducer <- makeProducer
      _ <- fakeProducer.produce(record)
      metrics <- reportedMetrics
    } yield metrics must contain(ProducingRecord(record, attributes.toMap))
  }

  "report metric when message is produced successfully" in {
    for {
      promise <- Promise.make[Nothing, Unit]
      metadata = RecordMetadata(topic, partition, 0)
      internal = new Producer {
        override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, IO[ProducerError, RecordMetadata]] =
          UIO(UIO(metadata))
      }
      producer = producerFrom(internal)
      _ <- promise.succeed(())
      _ <- producer.produceAsync(record).tap(_.tap(_ => adjustTestClock(1.second)))
      metrics <- reportedMetrics
    } yield metrics must contain(RecordProduced(metadata, attributes.toMap, FiniteDuration(0, TimeUnit.SECONDS)))
  }

  "report metric when produce fails" in (
    for {
      internal <- FakeProducer.make
      producer = producerFrom(internal.failing)
      _ <- producer.produce(record).either
      _ <- adjustTestClock(1.second)
      metrics <- reportedMetrics
    } yield metrics must contain(beLike[GreyhoundMetric] {
      case pf: ProduceFailed => pf.attributes === attributes.toMap
    }))

  private def reportedMetrics =
    TestMetrics.reported

  private def producerFrom(underlying: Producer) =
    ReportingProducer(underlying, attributes:_*)

  private def makeProducer =
    FakeProducer.make.map(ReportingProducer(_, attributes:_*))

  private def adjustTestClock(by: Duration): URIO[TestClock, Unit] =
    TestClock.adjust(by)
}

object ReportingProducerTest {
  val topic = "topic"
  val partition = 0
  val attributes = Seq("attr1" -> "attr1-value")
  val record = ProducerRecord(topic, Chunk.empty, partition = Some(partition))
}
