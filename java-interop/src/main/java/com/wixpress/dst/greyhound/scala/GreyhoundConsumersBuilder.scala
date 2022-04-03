package com.wixpress.dst.greyhound.java

import java.util.concurrent.Executor

import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.consumer.batched.{BatchConsumer, BatchConsumerConfig, BatchEventLoopConfig}
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.{NonBlockingBackoffPolicy, RetryConfig => CoreRetryConfig, RetryConfigForTopic}
import com.wixpress.dst.greyhound.core.consumer.{RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{consumer, Group, NonEmptySet, Topic}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio._
import zio.blocking.Blocking.Service.live.blockingExecutor

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

class GreyhoundConsumersBuilder(val config: GreyhoundConfig) {

  private val consumers = ListBuffer.empty[GreyhoundConsumer[_, _]]

  private val batchConsumers = ListBuffer.empty[GreyhoundBatchConsumer[_, _]]

  def withConsumer(consumer: GreyhoundConsumer[_, _]): GreyhoundConsumersBuilder = synchronized {
    consumers += consumer
    this
  }

  def withBatchConsumer(consumer: GreyhoundBatchConsumer[_, _]): GreyhoundConsumersBuilder = {
    batchConsumers += consumer
    this
  }

  def build(): GreyhoundConsumers = config.runtime.unsafeRun {
    for {
      runtime <- ZIO.runtime[Env]
      executor = createExecutor
      makeConsumer: ZManaged[Any with Env with GreyhoundMetrics, Throwable, immutable.Iterable[RecordConsumer[Any with Env]]] =
        ZManaged.foreach(handlers(executor, runtime)) {
          case (group: Group, javaConsumerConfig: JavaConsumerConfig) =>
            import javaConsumerConfig._
            RecordConsumer.make(
              RecordConsumerConfig(
                config.bootstrapServers,
                group,
                Topics(initialTopics),
                offsetReset = offsetReset,
                retryConfig = retryConfig,
                extraProperties = config.extraProperties
              ),
              handler
            )
        }
      makeBatchConsumer = ZManaged.foreach(batchConsumers.toSeq) { batchConsumer =>
        val batchConsumerConfig = BatchConsumerConfig(
          bootstrapServers = config.bootstrapServers,
          groupId = batchConsumer.group,
          initialSubscription = Topics(Set(batchConsumer.initialTopic)),
          retryConfig = batchConsumer.retryConfig,
          clientId = batchConsumer.clientId,
          eventLoopConfig = BatchEventLoopConfig.Default,
          offsetReset = convert(batchConsumer.offsetReset),
          extraProperties = config.extraProperties,
          userProvidedListener = batchConsumer.userProvidedListener,
          resubscribeTimeout = batchConsumer.resubscribeTimeout,
          initialOffsetsSeek = batchConsumer.initialOffsetsSeek
        )
        BatchConsumer.make(batchConsumerConfig, batchConsumer.batchRecordHandler(executor, runtime))
      }

      reservation <- makeConsumer.reserve
      consumers   <- reservation.acquire

      batchReservation <- makeBatchConsumer.reserve
      batchConsumer    <- batchReservation.acquire

    } yield new GreyhoundConsumers {
      override def pause(): Unit = runtime.unsafeRun {
        ZIO.foreach(consumers)(_.pause) *> ZIO.foreach(consumers)(_.pause)
      }

      override def resume(): Unit = runtime.unsafeRun {
        ZIO.foreach(consumers)(_.resume) *> ZIO.foreach(consumers)(_.resume)
      }

      override def isAlive(): Boolean = runtime.unsafeRun {
        ZIO.foreach(consumers)(_.isAlive).map(_.forall(_ == true)) *> ZIO.foreach(batchConsumer)(_.isAlive).map(_.forall(_ == true))
      }

      override def close(): Unit = runtime.unsafeRun {
        reservation.release(Exit.Success(())) *> batchReservation.release(Exit.Success(())).unit
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
      acc +
        (group ->
          JavaConsumerConfig(
            offsetReset = offsetReset,
            initialTopics = Set(consumer.initialTopic),
            handler = consumer.recordHandler(executor, runtime),
            retryConfig = convertRetryConfig(consumer.retryConfig)
          ))
    }

  private def convert(offsetReset: OffsetReset): core.consumer.OffsetReset =
    offsetReset match {
      case OffsetReset.Earliest => core.consumer.OffsetReset.Earliest
      case OffsetReset.Latest   => core.consumer.OffsetReset.Latest
    }

  private def convertRetryConfig(retryConfig: Option[RetryConfig]): Option[CoreRetryConfig] = {
    import scala.collection.JavaConverters._

    retryConfig.map(config => {
      val forTopic =
        RetryConfigForTopic(() => config.blockingBackoffs().asScala, NonBlockingBackoffPolicy(config.nonBlockingBackoffs.asScala))

      CoreRetryConfig({ case _ => forTopic }, None)
    })
  }
}

case class JavaConsumerConfig(
  offsetReset: consumer.OffsetReset,
  initialTopics: NonEmptySet[Topic],
  handler: CoreRecordHandler[Any, Any, Chunk[Byte], Chunk[Byte]],
  retryConfig: Option[CoreRetryConfig]
)
