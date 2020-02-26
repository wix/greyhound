package com.wixpress.dst.greyhound.java

import java.util.concurrent.CompletableFuture

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord, ReportingProducer}
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{ProducerRecord => KafkaProducerRecord}
import org.apache.kafka.common.serialization.{Serializer => KafkaSerializer}
import zio.{Exit, Promise, ZIO}

class GreyhoundProducerBuilder(val config: GreyhoundConfig) {
  def build: GreyhoundProducer = config.runtime.unsafeRun {
    for {
      shutdownSignal <- Promise.make[Nothing, Unit]
      promise <- Promise.make[Nothing, Producer[Env]]
      producerConfig = ProducerConfig(config.bootstrapServers)
      fiber <- Producer.make(producerConfig).use { producer =>
        promise.succeed(ReportingProducer(producer)) *>
          shutdownSignal.await
      }.fork
      runtime <- ZIO.runtime[Env]
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

      override def close(): Unit = runtime.unsafeRun {
        shutdownSignal.succeed(()).unit *> fiber.join
      }
    }
  }

}
