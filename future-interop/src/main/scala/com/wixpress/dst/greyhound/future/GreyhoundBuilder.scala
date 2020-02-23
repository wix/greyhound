package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ParallelConsumer, ParallelConsumerConfig, RecordHandler}
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Promise, ZIO}

import scala.concurrent.Future

trait Greyhound {
  def producer(config: GreyhoundProducerConfig): Future[GreyhoundProducer]
  def shutdown: Future[Unit]
}

case class GreyhoundBuilder(config: GreyhoundConfig,
                            consumers: Set[GreyhoundConsumer[_, _]] = Set.empty) {

  def withConsumer[K, V](consumer: GreyhoundConsumer[K, V]): GreyhoundBuilder =
    copy(consumers = consumers + consumer)

  def build: Future[Greyhound] = GreyhoundRuntime.Live.unsafeRunToFuture {
    for {
      ready <- Promise.make[Nothing, Unit]
      shutdownSignal <- Promise.make[Nothing, Unit]
      bootstrapServers = config.bootstrapServers
      handlers = consumers.groupBy(_.group).mapValues { groupConsumers =>
        groupConsumers.foldLeft[Handler[Env]](RecordHandler.empty) { (acc, consumer) =>
          acc combine consumer.recordHandler
        }
      }
      consumerConfig = ParallelConsumerConfig(bootstrapServers)
      makeConsumers = ParallelConsumer.make(consumerConfig, handlers)
      _ <- makeConsumers.use_ {
        ready.succeed(()) *>
          shutdownSignal.await
      }.fork
      _ <- ready.await
      runtime <- ZIO.runtime[Env]
    } yield new Greyhound {
      override def producer(config: GreyhoundProducerConfig): Future[GreyhoundProducer] =
        runtime.unsafeRunToFuture {
          for {
            promise <- Promise.make[Nothing, Producer[Env]]
            _ <- Producer.make(ProducerConfig(bootstrapServers)).use { producer =>
              promise.succeed(ReportingProducer(producer)) *>
                shutdownSignal.await
            }.fork
            producer <- promise.await
          } yield new GreyhoundProducer {
            override def produce[K, V](record: ProducerRecord[K, V],
                                       keySerializer: Serializer[K],
                                       valueSerializer: Serializer[V]): Future[RecordMetadata] =
              runtime.unsafeRunToFuture(producer.produce(record, keySerializer, valueSerializer))
          }
        }

      override def shutdown: Future[Unit] =
        runtime.unsafeRunToFuture(shutdownSignal.succeed(()).unit)
    }
  }

}

case class GreyhoundConfig(bootstrapServers: Set[String])
