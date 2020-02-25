package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ParallelConsumer, ParallelConsumerConfig, RecordHandler}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Promise, ZIO}

import scala.concurrent.Future

trait GreyhoundConsumers {
  def shutdown: Future[Unit]
}

case class GreyhoundConsumersBuilder(config: GreyhoundConfig,
                                     consumers: Set[GreyhoundConsumer[_, _]] = Set.empty) {

  def withConsumer[K, V](consumer: GreyhoundConsumer[K, V]): GreyhoundConsumersBuilder =
    copy(consumers = consumers + consumer)

  def build: Future[GreyhoundConsumers] = config.runtime.unsafeRunToFuture {
    for {
      ready <- Promise.make[Nothing, Unit]
      shutdownSignal <- Promise.make[Nothing, Unit]
      handlers = consumers.groupBy(_.group).mapValues { groupConsumers =>
        groupConsumers.foldLeft[Handler[Env]](RecordHandler.empty) { (acc, consumer) =>
          acc combine consumer.recordHandler
        }
      }
      consumerConfig = ParallelConsumerConfig(config.bootstrapServers)
      makeConsumers = ParallelConsumer.make(consumerConfig, handlers)
      fiber <- makeConsumers.use_ {
        ready.succeed(()) *>
          shutdownSignal.await
      }.fork
      _ <- ready.await
      runtime <- ZIO.runtime[Env]
    } yield new GreyhoundConsumers {
      override def shutdown: Future[Unit] = runtime.unsafeRunToFuture {
        shutdownSignal.succeed(()) *> fiber.join
      }
    }
  }

}
