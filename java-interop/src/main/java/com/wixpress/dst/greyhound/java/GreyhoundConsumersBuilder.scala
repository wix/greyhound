package com.wixpress.dst.greyhound.java

import java.util.concurrent.Executor

import com.wixpress.dst.greyhound.core.Group
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ParallelConsumer, ParallelConsumerConfig}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio._
import zio.blocking.blockingExecutor

import scala.collection.mutable.ListBuffer

class GreyhoundConsumersBuilder(val config: GreyhoundConfig) {

  private val consumers = ListBuffer.empty[GreyhoundConsumer[_, _]]

  def withConsumer(consumer: GreyhoundConsumer[_, _]): GreyhoundConsumersBuilder = synchronized {
    consumers += consumer
    this
  }

  def build(): GreyhoundConsumers = config.runtime.unsafeRun {
    for {
      runtime <- ZIO.runtime[Env]
      executor <- createExecutor
      consumerConfig = ParallelConsumerConfig(config.bootstrapServers)
      makeConsumer = ParallelConsumer.make(consumerConfig, handlers(executor))
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

  private def createExecutor =
    blockingExecutor.map { executor =>
      new Executor {
        override def execute(command: Runnable): Unit =
          executor.submit(command)
      }
    }

  private def handlers(executor: Executor) =
    consumers.foldLeft(Map.empty[Group, Handler[Env]]) { (acc, consumer) =>
      val group = consumer.group
      val handler = consumer.recordHandler(executor)
      val combined = acc.get(group).foldLeft(handler)(_ combine _)
      acc + (group -> combined)
    }

}
