package com.wixpress.dst.greyhound.java

import java.util.concurrent.Executor

import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.{Group, NonEmptySet, Topic, consumer}
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio._
import zio.blocking.Blocking.Service.live.blockingExecutor
import com.wixpress.dst.greyhound.core.ZManagedSyntax._

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
      executor = createExecutor
      makeConsumer = ZManaged.foreach(handlers(executor)) { case (group, (offsetReset, initialTopics, handler)) =>
        RecordConsumer.make(RecordConsumerConfig(config.bootstrapServers, group, initialTopics, offsetReset = offsetReset), handler)
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
      new Executor {
        override def execute(command: Runnable): Unit =
          blockingExecutor.submit(command)
      }

  private def handlers(executor: Executor): Map[Group, (consumer.OffsetReset, NonEmptySet[Topic], Handler[Env])] =
    consumers.foldLeft(Map.empty[Group, (core.consumer.OffsetReset, NonEmptySet[Topic], Handler[Env])]) { (acc, consumer) =>
      val (offsetReset, group) = (convert(consumer.offsetReset), consumer.group)
      acc + (group -> (offsetReset, Set(consumer.initialTopic), consumer.recordHandler(executor)))
    }

  private def convert(offsetReset: OffsetReset): core.consumer.OffsetReset =
    offsetReset match {
      case OffsetReset.Earliest => core.consumer.OffsetReset.Earliest
      case OffsetReset.Latest => core.consumer.OffsetReset.Latest
    }
}
