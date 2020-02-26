package com.wixpress.dst.greyhound.java

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ParallelConsumer, ParallelConsumerConfig}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio._

class GreyhoundConsumersBuilder(val config: GreyhoundConfig) {

  private var handlers = Map.empty[Group, Handler[Env]]

  def withConsumer[K, V](consumer: GreyhoundConsumer[K, V]): GreyhoundConsumersBuilder = synchronized {
    val group = consumer.group
    val handler = handlers.get(group).foldLeft(consumer.recordHandler)(_ combine _)
    handlers += (group -> handler)
    this
  }

  def build(): GreyhoundConsumers = config.runtime.unsafeRun {
    for {
      runtime <- ZIO.runtime[Env]
      consumerConfig = ParallelConsumerConfig(config.bootstrapServers)
      makeConsumer = ParallelConsumer.make(consumerConfig, handlers)
      reservation <- makeConsumer.reserve
      consumer <- reservation.acquire
    } yield new GreyhoundConsumers {
      override def pause(): Unit =
        runtime.unsafeRun(consumer.pause)

      override def resume(): Unit =
        runtime.unsafeRun(consumer.resume)

      override def isAlive(): Boolean =
        runtime.unsafeRun(consumer.isAlive)

      override def close(): Unit = runtime.unsafeRun {
        reservation.release(Exit.Success(())).unit
      }
    }
  }

}
