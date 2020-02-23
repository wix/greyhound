package com.wixpress.dst.greyhound.java

import java.util
import java.util.concurrent.CompletableFuture

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.consumer.EventLoop.Handler
import com.wixpress.dst.greyhound.core.consumer.{ParallelConsumer, ParallelConsumerConfig, RecordHandler => CoreRecordHandler}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord, ReportingProducer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{ProducerRecord => KafkaProducerRecord}
import org.apache.kafka.common.serialization.{Serializer => KafkaSerializer}
import zio.{Exit, Promise, ZIO}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class DefaultGreyhoundBuilder(val config: DefaultGreyhoundConfig) extends GreyhoundBuilder {

  private val runtime = GreyhoundRuntime.Live

  private val consumers = ListBuffer.empty[GreyhoundConsumer[_, _]]

  override def withConsumer[K, V](consumer: GreyhoundConsumer[K, V]): GreyhoundBuilder = {
    consumers += consumer
    this
  }

  override def build(): Greyhound = runtime.unsafeRun {
    for {
      ready <- Promise.make[Nothing, Unit]
      shutdownSignal <- Promise.make[Nothing, Unit]
      bootstrapServers = config.bootstrapServers.asScala.toSet
      handlers = consumers.groupBy(_.group).mapValues { groupConsumers =>
        groupConsumers.foldLeft[Handler[Env]](CoreRecordHandler.empty) { (acc, consumer) =>
          acc combine consumer.recordHandler
        }
      }
      consumerConfig = ParallelConsumerConfig(bootstrapServers)
      makeConsumers = ParallelConsumer.make(consumerConfig, handlers)
      _ <- makeConsumers.use_ {
        ready.succeed(()) *>
          shutdownSignal.await
      }.fork
      _ <- ready.await
      runtime <- ZIO.runtime[Env]
    } yield new Greyhound {
      override def producer(config: GreyhoundProducerConfig): GreyhoundProducer = runtime.unsafeRun {
        for {
          promise <- Promise.make[Nothing, Producer[Env]]
          _ <- Producer.make(ProducerConfig(bootstrapServers)).use { producer =>
            promise.succeed(ReportingProducer(producer)) *>
              shutdownSignal.await
          }.fork
        } yield new GreyhoundProducer {
          override def produce[K, V](record: KafkaProducerRecord[K, V],
                                     keySerializer: KafkaSerializer[K],
                                     valueSerializer: KafkaSerializer[V]): CompletableFuture[OffsetAndMetadata] = {
            val result = for {
              producer <- promise.await
              metadata <- producer.produce(
                ProducerRecord(
                  topic = record.topic,
                  value = record.value,
                  key = Option(record.key),
                  partition = Option(record.partition)), // TODO headers
                Serializer(keySerializer),
                Serializer(valueSerializer))
            } yield new OffsetAndMetadata(metadata.offset)

            val future = new CompletableFuture[OffsetAndMetadata]()
            runtime.unsafeRunAsync(result) {
              case Exit.Success(metadata) => future.complete(metadata)
              case Exit.Failure(cause) => future.completeExceptionally(cause.squash)
            }
            future
          }
        }
      }

      override def close(): Unit = runtime.unsafeRun {
        shutdownSignal.succeed(()).unit
      }
    }
  }

}

class DefaultGreyhoundConfig(val bootstrapServers: util.Set[String])
