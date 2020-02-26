package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ParallelConsumer, ParallelConsumerConfig}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Exit, ZIO}

import scala.concurrent.Future

trait GreyhoundConsumers extends Closeable

case class GreyhoundConsumersBuilder(config: GreyhoundConfig,
                                     handlers: Map[Group, Handler[Env]] = Map.empty) {

  def withConsumer[K, V](consumer: GreyhoundConsumer[K, V]): GreyhoundConsumersBuilder = {
    val group = consumer.group
    val handler = handlers.get(group).foldLeft(consumer.recordHandler)(_ combine _)
    copy(handlers = handlers + (group -> handler))
  }

  def build: Future[GreyhoundConsumers] = config.runtime.unsafeRunToFuture {
    for {
      runtime <- ZIO.runtime[Env]
      consumerConfig = ParallelConsumerConfig(config.bootstrapServers)
      makeConsumer = ParallelConsumer.make(consumerConfig, handlers)
      reservation <- makeConsumer.reserve
      _ <- reservation.acquire
    } yield new GreyhoundConsumers {
      override def shutdown: Future[Unit] =
        runtime.unsafeRunToFuture(reservation.release(Exit.Success(())).unit)
    }
  }

}
