package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit.MILLISECONDS

import _root_.zio.blocking.Blocking
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import zio.clock.Clock
import zio.{Chunk, Promise, ULayer, ZIO}

import scala.concurrent.duration.FiniteDuration

case class ReportingProducer[-R](internal: ProducerR[R])
  extends ProducerR[GreyhoundMetrics with Clock with R] {

  override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking with Clock with GreyhoundMetrics with R, ProducerError, Promise[ProducerError, RecordMetadata]] =
    (GreyhoundMetrics.report(ProducingRecord(record)) *>
      internal.produceAsync(record)
        .tap(_.await.timed.flatMap {
          case (duration, metadata) =>
            GreyhoundMetrics.report(RecordProduced(metadata, FiniteDuration(duration.toMillis, MILLISECONDS))).as(metadata)
        }.tapError { error =>
          GreyhoundMetrics.report(ProduceFailed(error))
        }.forkDaemon)
      )
}

object ReportingProducer {
  type Dependencies = ULayer[GreyhoundMetrics with zio.ZEnv]

  def apply[R](internal: ProducerR[R]): ReportingProducer[R] =
    new ReportingProducer(internal)
}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {

  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends ProducerMetric

  case class RecordProduced(metadata: RecordMetadata, duration: FiniteDuration) extends ProducerMetric

  case class ProduceFailed(error: ProducerError) extends ProducerMetric

}

