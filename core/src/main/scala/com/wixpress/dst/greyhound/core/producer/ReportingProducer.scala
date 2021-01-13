package com.wixpress.dst.greyhound.core.producer

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import _root_.zio.blocking.Blocking
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.producer.ProducerMetric._
import zio.clock.{Clock, currentTime}
import zio.{Chunk, IO, Ref, UIO, ULayer, URIO, ZIO}

import scala.concurrent.duration.FiniteDuration

case class ReportingProducer[-R](internal: ProducerR[R], attributes: Map[String, String])
  extends ProducerR[GreyhoundMetrics with Clock with R] {

  override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking with Clock with GreyhoundMetrics with R, ProducerError, IO[ProducerError, RecordMetadata]] =
    ReportingProducer.reporting[R](internal.produceAsync)(record, attributes)
}

object ReportingProducer {
  type Dependencies = ULayer[GreyhoundMetrics with zio.ZEnv]

  def apply[R](internal: ProducerR[R], attributes: (String, String)*): ReportingProducer[R] =
    new ReportingProducer(internal, attributes.toMap)

  def reporting[R](produceAsync: ProducerRecord[Chunk[Byte], Chunk[Byte]] => ZIO[Blocking with Clock with R, ProducerError, IO[ProducerError, RecordMetadata]])
                  (record: ProducerRecord[Chunk[Byte], Chunk[Byte]], attributes: Map[String, String] = Map.empty): ZIO[Blocking with Clock with GreyhoundMetrics with R, ProducerError, IO[ProducerError, RecordMetadata]] = {
    for {
      started <- currentTime(TimeUnit.MILLISECONDS)
      env <- ZIO.environment[Clock with GreyhoundMetrics with Blocking]
      _ <- GreyhoundMetrics.report(ProducingRecord(record, attributes))
      reported <- Ref.make(false)
      once = (block: UIO[Unit]) => reported.getAndUpdate(_ => true).negate.flatMap(ZIO.when(_)(block))
      promise <- produceAsync(record).map(_.tapBoth (
        error =>  once(GreyhoundMetrics.report(ProduceFailed(error, attributes)).provide(env)),
        metadata => once(currentTime(TimeUnit.MILLISECONDS).flatMap(ended =>
          GreyhoundMetrics.report(RecordProduced(metadata, attributes, FiniteDuration(ended - started, MILLISECONDS)))
        ).provide(env))
      ))
    } yield promise
  }
}

sealed trait ProducerMetric extends GreyhoundMetric

object ProducerMetric {

  case class ProducingRecord(record: ProducerRecord[Chunk[Byte], Chunk[Byte]], attributes: Map[String, String]) extends ProducerMetric

  case class RecordProduced(metadata: RecordMetadata, attributes: Map[String, String], duration: FiniteDuration) extends ProducerMetric

  case class ProduceFailed(error: ProducerError, attributes: Map[String, String]) extends ProducerMetric

}

