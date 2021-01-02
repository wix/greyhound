package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit.MILLISECONDS

import _root_.zio.blocking.Blocking
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import zio.clock.Clock
import zio.{Chunk, IO, ULayer, ZIO}

import scala.concurrent.duration.FiniteDuration

case class ReportingProducer[-R](internal: ProducerR[R], attributes: Map[String, String])
  extends ProducerR[GreyhoundMetrics with Clock with R] {

  override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking with Clock with GreyhoundMetrics with R, ProducerError, IO[ProducerError, RecordMetadata]] =
    GreyhoundMetrics.report(ProducingRecord(record, attributes)) *>
      internal.produceAsync(record)
        .tap(_.timed.flatMap {
          case (duration, metadata) =>
            GreyhoundMetrics.report(RecordProduced(metadata, attributes, FiniteDuration(duration.toMillis, MILLISECONDS))).as(metadata)
        }.tapError { error =>
          GreyhoundMetrics.report(ProduceFailed(error, attributes))
        }.forkDaemon)
}

object ReportingProducer {
  type Dependencies = ULayer[GreyhoundMetrics with zio.ZEnv]

  def apply[R](internal: ProducerR[R], attributes: (String, String)*): ReportingProducer[R] =
    new ReportingProducer(internal, attributes.toMap)
}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {

  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], attributes: Map[String, String]) extends ProducerMetric

  case class RecordProduced(metadata: RecordMetadata, attributes: Map[String, String], duration: FiniteDuration) extends ProducerMetric

  case class ProduceFailed(error: ProducerError, attributes: Map[String, String]) extends ProducerMetric

}

