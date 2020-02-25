package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Promise, ZIO}

import scala.concurrent.Future

trait GreyhoundProducer {
  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): Future[RecordMetadata]

  def shutdown: Future[Unit]
}

case class GreyhoundProducerBuilder(config: GreyhoundConfig) {
  def build: Future[GreyhoundProducer] = config.runtime.unsafeRunToFuture {
    for {
      shutdownSignal <- Promise.make[Nothing, Unit]
      promise <- Promise.make[Nothing, Producer[Env]]
      fiber <- Producer.make(ProducerConfig(config.bootstrapServers)).use { producer =>
        promise.succeed(ReportingProducer(producer)) *>
          shutdownSignal.await
      }.fork
      runtime <- ZIO.runtime[Env]
    } yield new GreyhoundProducer {
      override def produce[K, V](record: ProducerRecord[K, V],
                                 keySerializer: Serializer[K],
                                 valueSerializer: Serializer[V]): Future[RecordMetadata] =
        runtime.unsafeRunToFuture {
          promise.await.flatMap { producer =>
            producer.produce(record, keySerializer, valueSerializer)
          }
        }

      override def shutdown: Future[Unit] = runtime.unsafeRunToFuture {
        shutdownSignal.succeed(()) *> fiber.join
      }
    }
  }
}
