package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.producer.Producer.Producer
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import com.wixpress.dst.greyhound.core.producer.ReportingProducerTest._
import com.wixpress.dst.greyhound.core.testkit.{BaseTest, FakeProducer, TestMetrics}
import zio._
import zio.managed.UManaged
import zio.test.TestEnvironment

import scala.concurrent.duration.FiniteDuration
import zio.test.TestClock

class ReportingProducerTest extends BaseTest[TestClock with TestMetrics] {
  override def env = for {
    a <- testClock
    b <- TestMetrics.makeManagedEnv
  } yield a ++ b

  "delegate to internal producer" in {
    for {
      internal           <- FakeProducer.make
      internalPartitions <- internal.partitionsFor("some-topic")
      producer            = ReportingProducer(internal)
      _                  <- producer.produce(record)
      produced           <- internal.records.take
      partitions         <- producer.partitionsFor("some-topic")
    } yield {
      (produced must equalTo(record)) and (partitions === internalPartitions)
    }
  }

  "report metric when producing" in {
    for {
      fakeProducer <- makeProducer
      _            <- fakeProducer.produce(record)
      metrics      <- reportedMetrics
    } yield metrics must contain(ProducingRecord(record, underlyingAttributes ++ attributes.toMap))
  }

  "report metric when message is produced successfully" in {
    val syncDelay  = 100
    val asyncDelay = 200
    for {
      testClock <- ZIO.environment[TestClock].map(_.get)
      metadata   = RecordMetadata(topic, partition, 0)
      internal  <- FakeProducer.make(
                     r => testClock.adjust(syncDelay.millis).as(r),
                     m => testClock.adjust(asyncDelay.millis).as(m)
                   )
      producer   = producerFrom(internal)
      promise   <- producer.produceAsync(record)
      _         <- promise *> promise // evaluating the inner effect more than once should not report duplicate metrics
      metrics   <- reportedMetrics
    } yield metrics.filter(_.isInstanceOf[RecordProduced]) must
      exactly(RecordProduced(record, metadata, attributes.toMap, FiniteDuration(syncDelay + asyncDelay, TimeUnit.MILLISECONDS)))
  }

  "report metric when produce fails" in
    (for {
      internal <- FakeProducer.make
      producer  = producerFrom(internal.failing)
      _        <- producer.produce(record).either
      _        <- adjustTestClock(1.second)
      metrics  <- reportedMetrics
    } yield metrics must
      contain(beLike[GreyhoundMetric] { case pf: ProduceFailed => (pf.attributes === attributes.toMap) and pf.topic === record.topic }))

  private def reportedMetrics =
    TestMetrics.reported

  private def producerFrom(underlying: Producer) =
    ReportingProducer(underlying, attributes: _*)

  private def makeProducer =
    FakeProducer.make(attributes = underlyingAttributes).map(ReportingProducer(_, attributes: _*))

  private def adjustTestClock(by: Duration): URIO[TestClock, Unit] =
    TestClock.adjust(by)
}

object ReportingProducerTest {
  val topic                = "topic"
  val partition            = 0
  val attributes           = Seq("attr1" -> "attr1-value")
  val underlyingAttributes = Map("attr2" -> "attr2-value")
  val record               = ProducerRecord(topic, Chunk.empty, partition = Some(partition))
}
