package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Exit, ZIO}

import scala.concurrent.Future

trait GreyhoundProducer extends Closeable {
  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): Future[RecordMetadata]
}

case class GreyhoundProducerBuilder(config: GreyhoundConfig) {
  def withContextEncoding[C](encoder: ContextEncoder[C]): GreyhoundContextAwareProducerBuilder[C] =
    GreyhoundContextAwareProducerBuilder(config, encoder)

  def build: Future[GreyhoundProducer] = config.runtime.unsafeRunToFuture {
    for {
      runtime <- ZIO.runtime[Env]
      producerConfig = ProducerConfig(config.bootstrapServers)
      makeProducer = Producer.make(producerConfig).map(ReportingProducer(_))
      reservation <- makeProducer.reserve
      producer <- reservation.acquire
    } yield new GreyhoundProducer {
      override def produce[K, V](record: ProducerRecord[K, V],
                                 keySerializer: Serializer[K],
                                 valueSerializer: Serializer[V]): Future[RecordMetadata] =
        runtime.unsafeRunToFuture(producer.produce(record, keySerializer, valueSerializer))

      override def shutdown: Future[Unit] =
        runtime.unsafeRunToFuture(reservation.release(Exit.Success(())).unit)
    }
  }
}

// TODO remove duplication?
trait GreyhoundContextAwareProducer[C] extends Closeable {
  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V])
                   (implicit context: C): Future[RecordMetadata]
}

case class GreyhoundContextAwareProducerBuilder[C](config: GreyhoundConfig,
                                                   encoder: ContextEncoder[C]) {
  def build: Future[GreyhoundContextAwareProducer[C]] = config.runtime.unsafeRunToFuture {
    for {
      runtime <- ZIO.runtime[Env]
      producerConfig = ProducerConfig(config.bootstrapServers)
      makeProducer = Producer.make(producerConfig).map(ReportingProducer(_))
      reservation <- makeProducer.reserve
      producer <- reservation.acquire
    } yield new GreyhoundContextAwareProducer[C] {
      override def produce[K, V](record: ProducerRecord[K, V],
                                 keySerializer: Serializer[K],
                                 valueSerializer: Serializer[V])
                                (implicit context: C): Future[RecordMetadata] =
        runtime.unsafeRunToFuture {
          encoder.encode(record, context).flatMap { recordWithContext =>
            producer.produce(recordWithContext, keySerializer, valueSerializer)
          }
        }

      override def shutdown: Future[Unit] =
        runtime.unsafeRunToFuture(reservation.release(Exit.Success(())).unit)
    }
  }
}
