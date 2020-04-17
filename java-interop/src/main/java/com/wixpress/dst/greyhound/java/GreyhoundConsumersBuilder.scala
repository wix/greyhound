package com.wixpress.dst.greyhound.java

import java.util.concurrent.Executor

import com.wixpress.dst.greyhound.core
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
      makeConsumer = ZManaged.foreach(handlers(executor)) { case ((offsetReset, group), handler) =>
        ParallelConsumer.make(ParallelConsumerConfig(config.bootstrapServers, group, offsetReset = offsetReset), handler)
      }
      reservation <- makeConsumer.reserve
      consumers <- reservation.acquire
    } yield new GreyhoundConsumers {
      override def pause(): Unit =
        runtime.unsafeRun(ZIO.foreach(consumers)(_.pause))

      override def resume(): Unit =
        runtime.unsafeRun(ZIO.foreach(consumers)(_.resume))

      override def isAlive(): Boolean =
        runtime.unsafeRun(ZIO.foreach(consumers)(_.isAlive).map(_.forall(_ == true)))

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
    consumers.foldLeft(Map.empty[(core.consumer.OffsetReset, Group), Handler[Env]]) { (acc, consumer) =>
      val (offsetReset, group) = (convert(consumer.offsetReset), consumer.group)
      val handler = consumer.recordHandler(executor)
      val combined = acc.get((offsetReset, group)).foldLeft(handler)(_ combine _)
      acc + ((offsetReset, group) -> combined)
    }

  private def convert(offsetReset: OffsetReset): core.consumer.OffsetReset =
    offsetReset match {
      case OffsetReset.Earliest => core.consumer.OffsetReset.Earliest
      case OffsetReset.Latest => core.consumer.OffsetReset.Latest
    }
}
