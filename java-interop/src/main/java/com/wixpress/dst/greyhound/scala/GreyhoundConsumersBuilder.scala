package com.wixpress.dst.greyhound.java

import java.util.concurrent.{Executor, Executors}
import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.consumer.batched.{BatchConsumer, BatchConsumerConfig, BatchEventLoopConfig}
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.consumer.retry.{EmptyInfiniteBlockingBackoffPolicy, FiniteBlockingBackoffPolicy, InfiniteBlockingBackoffPolicy, NonBlockingBackoffPolicy, RetryConfigForTopic, RetryConfig => CoreRetryConfig}
import com.wixpress.dst.greyhound.core.consumer.{RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.{Group, NonEmptySet, Topic, consumer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio._

import scala.collection.mutable.ListBuffer

class GreyhoundConsumersBuilder(val config: GreyhoundConfig) {

  private val consumers        = ListBuffer.empty[GreyhoundConsumer[_, _]]
  private val blockingExecutor = Executors.newCachedThreadPool()

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
      runtime          <- ZIO.runtime[Env]
      executor          = createExecutor
      makeConsumer      =
        ZIO
          .foreach(handlers(executor, runtime).toSeq) {
            case (group: Group, javaConsumerConfig: JavaConsumerConfig) =>
              import javaConsumerConfig._
              RecordConsumer.make(
                handler = handler,
                config = RecordConsumerConfig(
                  config.bootstrapServers,
                  group,
                  Topics(initialTopics),
                  offsetReset = offsetReset,
                  retryConfig = retryConfig,
                  extraProperties = config.extraProperties
                )
              )
          }
          .provideSome[RecordConsumer.Env](ZLayer.succeed(Scope.global))
      makeBatchConsumer = ZIO.foreach(batchConsumers.toSeq) { batchConsumer =>
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

      consumers     <- makeConsumer
      batchConsumer <- makeBatchConsumer.provideSome[BatchConsumer.Env](ZLayer.succeed(Scope.global))

    } yield new GreyhoundConsumers {
      override def pause(): Unit = Unsafe.unsafe { implicit s =>
        runtime.unsafe
          .run(
            ZIO.foreach(consumers)(_.pause) *> ZIO.foreach(consumers)(_.pause)
          )
          .getOrThrowFiberFailure()
      }

      override def resume(): Unit = Unsafe.unsafe { implicit s =>
        runtime.unsafe
          .run(
            ZIO.foreach(consumers)(_.resume) *> ZIO.foreach(consumers)(_.resume)
          )
          .getOrThrowFiberFailure()
      }

      override def isAlive(): Boolean = Unsafe.unsafe { implicit s =>
        runtime.unsafe
          .run(
            ZIO.foreach(consumers)(_.isAlive).map(_.forall(_ == true)) *> ZIO.foreach(batchConsumer)(_.isAlive).map(_.forall(_ == true))
          )
          .getOrThrowFiberFailure()
      }

      override def close(): Unit = Unsafe.unsafe { implicit s =>
        runtime.unsafe
          .run(
            ZIO.foreach(batchConsumer)(_.shutdown(30.seconds)) *> ZIO.foreach(consumers)(_.shutdown())
          )
          .getOrThrowFiberFailure()
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
        RetryConfigForTopic(
          FiniteBlockingBackoffPolicy(config.blockingBackoffs().asScala.toList),
          EmptyInfiniteBlockingBackoffPolicy,
          NonBlockingBackoffPolicy(config.nonBlockingBackoffs.asScala.toList)
        )

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
