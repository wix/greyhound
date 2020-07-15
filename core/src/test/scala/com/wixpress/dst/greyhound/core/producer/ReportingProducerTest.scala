package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit

import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import com.wixpress.dst.greyhound.core.producer.ReportingProducerTest._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, FakeProducer, TestMetrics}
import zio._
import zio.blocking.Blocking
import zio.duration._
import zio.test.environment.{TestClock, TestEnvironment}

import scala.concurrent.duration.FiniteDuration

class ReportingProducerTest extends BaseTest[Blocking] {
  override def env: UManaged[Blocking] = testEnv

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

  "report metric when producing" in testEnv.use(deps =>
    for {
      fakeProducer <- makeProducer(deps)
      _ <- fakeProducer.produce(record)
      metrics <- reportedMetrics(deps)
    } yield metrics must contain(ProducingRecord(record))
  )

  "report metric when message is produced successfully" in testEnv.use(deps =>
    for {
      promise <- Promise.make[Nothing, Unit]
      metadata = RecordMetadata(topic, partition, 0)
      internal = new Producer {
        override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, ZIO[Any, ProducerError, RecordMetadata]] =
          promise.await.as(UIO(metadata))
      }
      producer = producerFrom(deps, internal)
      _ <- promise.succeed(())
      _ <- producer.produceAsync(record).tap(_.tap(_ => adjustTestClock(deps, 1.second)))
      metrics <- reportedMetrics(deps)
    } yield metrics must contain(RecordProduced(metadata, FiniteDuration(0, TimeUnit.SECONDS)))
  )

  "report metric when produce fails" in testEnv.use(deps =>
    for {
      internal <- FakeProducer.make
      producer = producerFrom(deps, internal.failing)
      _ <- producer.produce(record).either
      _ <- adjustTestClock(deps, 1.second)
      metrics <- reportedMetrics(deps)
    } yield metrics must contain(beAnInstanceOf[ProduceFailed])
  )

  private def reportedMetrics(deps: _root_.zio.test.environment.TestEnvironment with TestMetrics) =
    TestMetrics.reported.provideLayer(ZLayer.succeedMany(deps))

  private def producerFrom(deps: TestEnvironment with TestMetrics, underlying: Producer) =
    ReportingProducer(underlying, ZLayer.succeedMany(deps))

  private def makeProducer(deps: TestEnvironment with TestMetrics) =
    FakeProducer.make.map(p => producerFrom(deps, p))

  private def adjustTestClock(deps: TestEnvironment with TestMetrics, by: Duration) =
    TestClock.adjust(by).provideLayer(ZLayer.succeedMany(deps))
}

object ReportingProducerTest {
  val topic = "topic"
  val partition = 0
  val record = ProducerRecord(topic, Chunk.empty, partition = Some(partition))
}
