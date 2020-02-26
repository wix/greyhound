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
      ready <- Promise.make[Nothing, Unit]
      shutdownSignal <- Promise.make[Nothing, Unit]
      consumerConfig = ParallelConsumerConfig(config.bootstrapServers)
      fiber <- ParallelConsumer.make(consumerConfig, handlers).use_ {
        ready.succeed(()) *> shutdownSignal.await
      }.fork
      _ <- ready.await
      runtime <- ZIO.runtime[Env]
    } yield new GreyhoundConsumers {
      override def close(): Unit = runtime.unsafeRun {
        shutdownSignal.succeed(()).unit *> fiber.join
      }
    }
  }

}
