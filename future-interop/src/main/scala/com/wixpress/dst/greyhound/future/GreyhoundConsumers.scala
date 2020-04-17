package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{OffsetReset, ParallelConsumer, ParallelConsumerConfig}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio.{Exit, ZIO, ZManaged}

import scala.concurrent.Future

trait GreyhoundConsumers extends Closeable {
  def pause: Future[Unit]

  def resume: Future[Unit]

  def isAlive: Future[Boolean]
}

case class GreyhoundConsumersBuilder(config: GreyhoundConfig,
                                     handlers: Map[(OffsetReset, Group), Handler[Env]] = Map.empty) {

  def withConsumer[K, V](consumer: GreyhoundConsumer[K, V]): GreyhoundConsumersBuilder = {
    val (group, offsetReset) = (consumer.group, consumer.offsetReset)
    val handler = handlers.get((offsetReset, group)).foldLeft(consumer.recordHandler)(_ combine _)
    copy(handlers = handlers + ((offsetReset, group) -> handler))
  }

  def build: Future[GreyhoundConsumers] = config.runtime.unsafeRunToFuture {
    for {
      runtime <- ZIO.runtime[Env]
      makeConsumer = ZManaged.foreach(handlers) { case ((offsetReset, group), handler) =>
        ParallelConsumer.make(ParallelConsumerConfig(config.bootstrapServers, group, offsetReset = offsetReset), handler)
      }
      reservation <- makeConsumer.reserve
      consumers <- reservation.acquire
    } yield new GreyhoundConsumers {
      override def pause: Future[Unit] =
        runtime.unsafeRunToFuture(ZIO.foreach(consumers)(_.pause).unit)

      override def resume: Future[Unit] =
        runtime.unsafeRunToFuture(ZIO.foreach(consumers)(_.resume).unit)

      override def isAlive: Future[Boolean] =
        runtime.unsafeRunToFuture(ZIO.foreach(consumers)(_.isAlive).map(_.forall(_ == true)))

      override def shutdown: Future[Unit] =
        runtime.unsafeRunToFuture(reservation.release(Exit.Success(())).unit)
    }
  }

}
