package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import zio.clock.Clock
import zio.duration.Duration
import zio.{Chunk, ZIO}

case class ReportingProducer[-R](internal: Producer[R])
  extends Producer[R with GreyhoundMetrics with Clock] {

  override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R with GreyhoundMetrics with Clock, ProducerError, RecordMetadata] =
    Metrics.report(ProducingRecord(record)) *>
      internal.produce(record).timed.flatMap {
        case (duration, metadata) =>
          Metrics.report(RecordProduced(metadata, duration)).as(metadata)
      }.tapError { error =>
        Metrics.report(ProduceFailed(error))
      }

}

sealed trait ProducerMetric extends GreyhoundMetric
case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends ProducerMetric
case class RecordProduced(metadata: RecordMetadata, duration: Duration) extends ProducerMetric
case class ProduceFailed(error: ProducerError) extends ProducerMetric
