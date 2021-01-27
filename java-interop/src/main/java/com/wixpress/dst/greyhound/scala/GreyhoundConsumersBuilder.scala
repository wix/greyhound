package com.wixpress.dst.greyhound.java

import java.util.concurrent.Executor

import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.{NonBlockingBackoffPolicy, RetryConfigForTopic, RetryConfig => CoreRetryConfig}
import com.wixpress.dst.greyhound.core.consumer.{RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.{Group, NonEmptySet, Topic, consumer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio._
import zio.blocking.Blocking.Service.live.blockingExecutor

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
      makeConsumer = ZManaged.foreach(handlers(executor, runtime)) { case (group, javaConsumerConfig) =>
        import javaConsumerConfig._
        RecordConsumer.make(RecordConsumerConfig(config.bootstrapServers, group, Topics(initialTopics), offsetReset = offsetReset, retryConfig = retryConfig, extraProperties = config.extraProperties), handler)
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

  private def handlers(executor: Executor, runtime: zio.Runtime[GreyhoundRuntime.Env]): Map[Group, JavaConsumerConfig] =
    consumers.foldLeft(Map.empty[Group, JavaConsumerConfig]) { (acc, consumer) =>
      val (offsetReset, group) = (convert(consumer.offsetReset), consumer.group)
      acc + (group -> JavaConsumerConfig(offsetReset, Set(consumer.initialTopic), consumer.recordHandler(executor, runtime), convertRetryConfig(consumer.retryConfig)))
    }

  private def convert(offsetReset: OffsetReset): core.consumer.OffsetReset =
    offsetReset match {
      case OffsetReset.Earliest => core.consumer.OffsetReset.Earliest
      case OffsetReset.Latest => core.consumer.OffsetReset.Latest
    }

  private def convertRetryConfig(retryConfig: Option[RetryConfig]): Option[CoreRetryConfig] = {
    import scala.collection.JavaConverters._

    retryConfig.map(config ⇒ {
      val forTopic = RetryConfigForTopic(
        () ⇒ config.blockingBackoffs().asScala,
        NonBlockingBackoffPolicy(config.nonBlockingBackoffs.asScala))

      CoreRetryConfig({ case _ => forTopic }, None)
    })
  }
}

case class JavaConsumerConfig(offsetReset: consumer.OffsetReset, initialTopics: NonEmptySet[Topic], handler: CoreRecordHandler[Any, Any, Chunk[Byte], Chunk[Byte]], retryConfig: Option[CoreRetryConfig])