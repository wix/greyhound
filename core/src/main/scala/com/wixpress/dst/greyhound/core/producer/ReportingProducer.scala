package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit.MILLISECONDS

import _root_.zio.blocking.Blocking
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import zio.clock.Clock
import zio.{Chunk, IO, ULayer, ZIO}

import scala.concurrent.duration.FiniteDuration

case class ReportingProducer[-R](internal: ProducerR[R], cluster: String = "default")
  extends ProducerR[GreyhoundMetrics with Clock with R] {

  override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking with Clock with GreyhoundMetrics with R, ProducerError, IO[ProducerError, RecordMetadata]] =
    GreyhoundMetrics.report(ProducingRecord(record, cluster)) *>
      internal.produceAsync(record)
        .tap(_.timed.flatMap {
          case (duration, metadata) =>
            GreyhoundMetrics.report(RecordProduced(metadata, cluster, FiniteDuration(duration.toMillis, MILLISECONDS))).as(metadata)
        }.tapError { error =>
          GreyhoundMetrics.report(ProduceFailed(error, cluster))
        }.forkDaemon)
}

object ReportingProducer {
  type Dependencies = ULayer[GreyhoundMetrics with zio.ZEnv]

  def apply[R](internal: ProducerR[R], cluster: String = "default"): ReportingProducer[R] =
    new ReportingProducer(internal, cluster)
}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {

  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], cluster: String) extends ProducerMetric

  case class RecordProduced(metadata: RecordMetadata, cluster: String, duration: FiniteDuration) extends ProducerMetric

  case class ProduceFailed(error: ProducerError, cluster: String) extends ProducerMetric

}

