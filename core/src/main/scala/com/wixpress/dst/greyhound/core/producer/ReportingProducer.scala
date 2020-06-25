package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import com.wixpress.dst.greyhound.core.producer.ReportingProducer.Dependencies
import zio.clock.Clock
import zio.{Chunk, ULayer, ZIO}

import scala.concurrent.duration.FiniteDuration

case class ReportingProducer(internal: Producer, layers: Dependencies)
  extends Producer {

  override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Any, ProducerError, RecordMetadata] =
    (GreyhoundMetrics.report(ProducingRecord(record)) *>
      internal.produce(record).timed.flatMap {
        case (duration, metadata) =>
          GreyhoundMetrics.report(RecordProduced(metadata, FiniteDuration(duration.toMillis, MILLISECONDS))).as(metadata)
      }.tapError { error =>
        GreyhoundMetrics.report(ProduceFailed(error))
      }).provideLayer(layers)
}

object ReportingProducer {
  type Dependencies = ULayer[GreyhoundMetrics with Clock]

  def apply(internal: Producer, layers: ULayer[GreyhoundMetrics with Clock] = GreyhoundMetrics.liveLayer ++ Clock.live): ReportingProducer =
    new ReportingProducer(internal, layers)
}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {

  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends ProducerMetric

  case class RecordProduced(metadata: RecordMetadata, duration: FiniteDuration) extends ProducerMetric

  case class ProduceFailed(error: ProducerError) extends ProducerMetric

}

