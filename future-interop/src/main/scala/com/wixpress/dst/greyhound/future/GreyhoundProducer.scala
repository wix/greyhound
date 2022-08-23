package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{ZIO, ZLayer}

import scala.concurrent.Future

trait GreyhoundProducer extends Closeable {
  def produce[K, V](record: ProducerRecord[K, V], keySerializer: Serializer[K], valueSerializer: Serializer[V]): Future[RecordMetadata]
}

case class GreyhoundProducerBuilder(config: GreyhoundConfig, mutateProducer: ProducerConfig => ProducerConfig = identity) {
  def withContextEncoding[C](encoder: ContextEncoder[C]): GreyhoundContextAwareProducerBuilder[C] =
    GreyhoundContextAwareProducerBuilder(config, encoder)

  def build: Future[GreyhoundProducer] = config.runtime.unsafeRunToFuture {
    for {
      runtime       <- ZIO.runtime[Env]
      producerConfig = mutateProducer(ProducerConfig(config.bootstrapServers))
      producer   <- Producer.makeR[Any](producerConfig).map(ReportingProducer(_)).provideLayer(ZLayer.succeed(zio.Scope.global))
    } yield new GreyhoundProducer {
      override def produce[K, V](
        record: ProducerRecord[K, V],
        keySerializer: Serializer[K],
        valueSerializer: Serializer[V]
      ): Future[RecordMetadata] =
        config.runtime.unsafeRunToFuture(producer.produce(record, keySerializer, valueSerializer))

      override def shutdown: Future[Unit] =
        config.runtime.unsafeRunToFuture(producer.shutdown.unit)
    }
  }
}

// TODO remove duplication?
trait GreyhoundContextAwareProducer[C] extends Closeable {
  def produce[K, V](record: ProducerRecord[K, V], keySerializer: Serializer[K], valueSerializer: Serializer[V])(
    implicit context: C
  ): Future[RecordMetadata]
}

case class GreyhoundContextAwareProducerBuilder[C](config: GreyhoundConfig, encoder: ContextEncoder[C]) {
  def build: Future[GreyhoundContextAwareProducer[C]] = config.runtime.unsafeRunToFuture {
    for {
      runtime       <- ZIO.runtime[Env]
      producerConfig = ProducerConfig(config.bootstrapServers)
      producer   <- Producer.makeR[Any](producerConfig).map(ReportingProducer(_)).provideLayer(ZLayer.succeed(zio.Scope.global))
    } yield new GreyhoundContextAwareProducer[C] {
      override def produce[K, V](record: ProducerRecord[K, V], keySerializer: Serializer[K], valueSerializer: Serializer[V])(
        implicit context: C
      ): Future[RecordMetadata] =
        config.runtime.unsafeRunToFuture {
          encoder.encode(record, context).flatMap { recordWithContext =>
            producer.produce(recordWithContext, keySerializer, valueSerializer)
          }
        }

      override def shutdown: Future[Unit] =
        config.runtime.unsafeRunToFuture(producer.shutdown.unit)
    }
  }
}
