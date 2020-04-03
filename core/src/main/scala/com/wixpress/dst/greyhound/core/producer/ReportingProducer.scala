package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import zio.clock.Clock
import zio.{Chunk, ZIO}

import scala.concurrent.duration.FiniteDuration

case class ReportingProducer[-R](internal: Producer[R])
  extends Producer[R with GreyhoundMetrics with Clock] {

  override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R with GreyhoundMetrics with Clock, ProducerError, RecordMetadata] =
    Metrics.report(ProducingRecord(record)) *>
      internal.produce(record).timed.flatMap {
        case (duration, metadata) =>
          Metrics.report(RecordProduced(metadata, FiniteDuration(duration.toMillis, MILLISECONDS))).as(metadata)
      }.tapError { error =>
        Metrics.report(ProduceFailed(error))
      }

}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {
  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends ProducerMetric
  case class RecordProduced(metadata: RecordMetadata, duration: FiniteDuration) extends ProducerMetric
  case class ProduceFailed(error: ProducerError) extends ProducerMetric
}
