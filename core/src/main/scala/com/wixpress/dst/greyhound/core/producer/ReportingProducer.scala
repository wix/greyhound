package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit.MILLISECONDS

import _root_.zio.blocking.Blocking
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import com.wixpress.dst.greyhound.core.producer.ReportingProducer.Dependencies
import zio.clock.Clock
import zio.console.Console
import zio.random.Random
import zio.system.System
import zio.{Chunk, Promise, UIO, ULayer, ZIO}

import scala.concurrent.duration.FiniteDuration

case class ReportingProducer(internal: Producer, layers: Dependencies)
  extends Producer {
  override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, Promise[ProducerError, RecordMetadata]] =
    (GreyhoundMetrics.report(ProducingRecord(record)) *>
      internal.produceAsync(record)
        .tap(kafkaResultIO =>
          kafkaResultIO.await.timed.flatMap {
            case (duration, metadata) =>
              GreyhoundMetrics.report(RecordProduced(metadata, FiniteDuration(duration.toMillis, MILLISECONDS))).as(metadata)
          }.tapError { error =>
              GreyhoundMetrics.report(ProduceFailed(error))
          }.provideLayer(layers).forkDaemon)
      ).provideLayer(layers)
}

object ReportingProducer {
  type Dependencies = ULayer[GreyhoundMetrics with zio.ZEnv]

  def apply(internal: Producer, layers: ULayer[GreyhoundMetrics with zio.ZEnv] = GreyhoundMetrics.liveLayer ++ zenv): ReportingProducer =
    new ReportingProducer(internal, layers)

  private val zenv = Clock.live ++ System.live ++ Random.live ++ Blocking.live ++ Console.live
}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {

  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) extends ProducerMetric

  case class RecordProduced(metadata: RecordMetadata, duration: FiniteDuration) extends ProducerMetric

  case class ProduceFailed(error: ProducerError) extends ProducerMetric

}

