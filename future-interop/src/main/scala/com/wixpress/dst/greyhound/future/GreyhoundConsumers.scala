package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ParallelConsumer, ParallelConsumerConfig}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Exit, ZIO}

import scala.concurrent.Future

trait GreyhoundConsumers extends Closeable {
  def pause: Future[Unit]

  def resume: Future[Unit]

  def isAlive: Future[Boolean]
}

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
      consumer <- reservation.acquire
    } yield new GreyhoundConsumers {
      override def pause: Future[Unit] =
        runtime.unsafeRunToFuture(consumer.pause)

      override def resume: Future[Unit] =
        runtime.unsafeRunToFuture(consumer.resume)

      override def isAlive: Future[Boolean] =
        runtime.unsafeRunToFuture(consumer.isAlive)

      override def shutdown: Future[Unit] =
        runtime.unsafeRunToFuture(reservation.release(Exit.Success(())).unit)
    }
  }

}
