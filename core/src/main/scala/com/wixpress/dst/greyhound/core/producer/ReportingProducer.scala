package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS
import com.wixpress.dst.greyhound.core.PartitionInfo
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import zio.{Chunk, IO, RIO, Trace, ULayer, ZIO}
import GreyhoundMetrics._

import scala.concurrent.duration.FiniteDuration
import zio.Clock.currentTime

case class ReportingProducer[-R](internal: ProducerR[R], extraAttributes: Map[String, String]) extends ProducerR[GreyhoundMetrics with R] {

  override def produceAsync(
    record: ProducerRecord[Chunk[Byte], Chunk[Byte]]
  )(implicit trace: Trace): ZIO[GreyhoundMetrics with R, ProducerError, IO[ProducerError, RecordMetadata]] =
    ReportingProducer.reporting[R](internal.produceAsync)(record, attributes)

  override def attributes: Map[String, String] = internal.attributes ++ extraAttributes

  override def partitionsFor(topic: String)(implicit trace: Trace): RIO[R with GreyhoundMetrics, Seq[PartitionInfo]] =
    internal
      .partitionsFor(topic)
      .reporting(ProducerGotPartitionsInfo(topic, attributes, _))
}

object ReportingProducer {
  type Dependencies = ULayer[GreyhoundMetrics]

  def apply[R](internal: ProducerR[R], attributes: (String, String)*): ReportingProducer[R] =
    new ReportingProducer(internal, attributes.toMap)

  def reporting[R](
    produceAsync: ProducerRecord[Chunk[Byte], Chunk[Byte]] => ZIO[
      R,
      ProducerError,
      IO[ProducerError, RecordMetadata]
    ]
  )(
    record: ProducerRecord[Chunk[Byte], Chunk[Byte]],
    attributes: Map[String, String] = Map.empty
  )(implicit trace: Trace): ZIO[GreyhoundMetrics with R, ProducerError, IO[ProducerError, RecordMetadata]] = {
    for {
      started   <- currentTime(TimeUnit.MILLISECONDS)
      env       <- ZIO.environment[GreyhoundMetrics]
      _         <- GreyhoundMetrics.report(ProducingRecord(record, attributes))
      onError   <- ZIO.memoize((error: ProducerError) =>
                     GreyhoundMetrics.report(ProduceFailed(error, record.topic, attributes)).provideEnvironment(env)
                   )
      onSuccess <-
        ZIO.memoize((metadata: RecordMetadata) =>
          currentTime(TimeUnit.MILLISECONDS)
            .flatMap(ended =>
              GreyhoundMetrics.report(RecordProduced(record, metadata, attributes, FiniteDuration(ended - started, MILLISECONDS)))
            )
            .provideEnvironment(env)
        )
      promise   <- produceAsync(record).map(_.tapBoth(onError, onSuccess))
    } yield promise
  }
}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {

  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], attributes: Map[String, String]) extends ProducerMetric

  case class RecordProduced(
    record: ProducerRecord[Chunk[Byte], Chunk[Byte]],
    metadata: RecordMetadata,
    attributes: Map[String, String],
    duration: FiniteDuration
  ) extends ProducerMetric

  case class ProduceFailed(error: ProducerError, topic: String, attributes: Map[String, String]) extends ProducerMetric

  case class ProducerGotPartitionsInfo(topic: String, attributes: Map[String, String], result: MetricResult[Throwable, Seq[PartitionInfo]])
      extends ProducerMetric

}
