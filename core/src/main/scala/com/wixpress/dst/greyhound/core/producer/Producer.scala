package com.wixpress.dst.greyhound.core.producer


import com.wixpress.dst.greyhound.core._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig => KafkaProducerConfig, ProducerRecord => KafkaProducerRecord, RecordMetadata => KafkaRecordMetadata}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import zio._
import zio.blocking.{Blocking, effectBlocking}
import zio.duration._

import scala.collection.JavaConverters._

trait ProducerR[-R] { self =>
  def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R with Blocking, ProducerError, IO[ProducerError, RecordMetadata]]

  def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R with Blocking, ProducerError, RecordMetadata] =
    for {
      promise <- produceAsync(record)
      res <- promise // promise is ZIO itself (the result of from Proimse.await)
    } yield res

  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V],
                    encryptor: Encryptor = NoOpEncryptor): ZIO[R with Blocking, ProducerError, RecordMetadata] =
    serialized(record, keySerializer, valueSerializer, encryptor)
      .mapError(SerializationError)
      .flatMap(produce)

  def produceAsync[K, V](record: ProducerRecord[K, V],
                         keySerializer: Serializer[K],
                         valueSerializer: Serializer[V],
                         encryptor: Encryptor = NoOpEncryptor): ZIO[R with Blocking, ProducerError, IO[ProducerError, RecordMetadata]] =
    serialized(record, keySerializer, valueSerializer, encryptor)
      .mapError(SerializationError)
      .flatMap(produceAsync)

  def shutdown: UIO[Unit] = UIO.unit

  def attributes: Map[String, String] = Map.empty

  private def serialized[V, K](record: ProducerRecord[K, V],
                               keySerializer: Serializer[K],
                               valueSerializer: Serializer[V],
                               encryptor: Encryptor) = {
    for {
      keyBytes <- keySerializer.serializeOpt(record.topic, record.key)
      valueBytes <- valueSerializer.serializeOpt(record.topic, record.value)
      serializedRecord = ProducerRecord(
        topic = record.topic,
        value = valueBytes,
        key = keyBytes,
        partition = record.partition,
        headers = record.headers)
      encyptedRecord <- encryptor.encrypt(serializedRecord)
    } yield encyptedRecord
  }
}


object Producer {
  private val serializer = new ByteArraySerializer
  type Producer = ProducerR[Any]

  def make(config: ProducerConfig, attrs: Map[String, String] = Map.empty): RManaged[Blocking, Producer] =
    makeR[Any](config, attrs)

  def makeR[R](config: ProducerConfig, attrs: Map[String, String] = Map.empty): RManaged[Blocking, ProducerR[R]] = {
    val acquire = effectBlocking(new KafkaProducer(config.properties, serializer, serializer))
    ZManaged.make(acquire)(producer => effectBlocking(producer.close()).ignore).map { producer =>
      new ProducerR[R] {
        private def recordFrom(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) =
          new KafkaProducerRecord(
            record.topic,
            record.partition.fold[Integer](null)(Integer.valueOf),
            record.key.fold[Array[Byte]](null)(_.toArray),
            record.value.map(_.toArray).orNull,
            headersFrom(record.headers).asJava)

        private def headersFrom(headers: Headers): Iterable[Header] =
          headers.headers.map {
            case (key, value) =>
              new RecordHeader(key, value.toArray)
          }

        override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking with R, ProducerError, IO[ProducerError, RecordMetadata]] =
          for {
            produceCompletePromise <- Promise.make[ProducerError, RecordMetadata]
            runtime <- ZIO.runtime[Any]
            _ <- effectBlocking(producer.send(recordFrom(record), new Callback {
              override def onCompletion(metadata: KafkaRecordMetadata, exception: Exception): Unit =
                runtime.unsafeRun {
                  (if (exception != null) produceCompletePromise.complete(ProducerError(exception))
                  else produceCompletePromise.succeed(RecordMetadata(metadata))) *>
                    config.onProduceListener(record)
                }
            }))
              .catchAll(e => produceCompletePromise.complete(ProducerError(e)))
          } yield (produceCompletePromise.await)

        override def attributes: Map[String, String] = attrs
      }
    }
  }
}

object ProducerR {
  implicit class Ops[R <: Has[_]](producer: ProducerR[R]) {
    // R1 is a work around to an apparent bug in Has.union ¯\_(ツ)_/¯
    // https://github.com/zio/zio/issues/3558#issuecomment-776051184
    def provide[R1 <: R : Tag](env: R1) = new ProducerR[Any] {
      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, IO[ProducerError, RecordMetadata]] =
        producer.produceAsync(record).provideSome[Blocking](_.union[R1](env))

      override def attributes: Map[String, String] = producer.attributes

      override def shutdown: UIO[Unit] = producer.shutdown
    }
    def onShutdown(onShutdown: => UIO[Unit]): ProducerR[R] = new ProducerR[R] {
      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R with Blocking, ProducerError, IO[ProducerError, RecordMetadata]] =
        producer.produceAsync(record)

      override def shutdown: UIO[Unit] = onShutdown *> producer.shutdown

      override def attributes: Map[String, String] = producer.attributes
    }

    def tapBoth(onError: (Topic, Cause[ProducerError]) => URIO[R, Unit], onSuccess: RecordMetadata => URIO[R, Unit]) = new ProducerR[R] {
      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R with Blocking, ProducerError, IO[ProducerError, RecordMetadata]] = {
        for {
          called <- Ref.make(false)
          once = ZIO.whenM(called.getAndUpdate(_ => true).negate)(_: URIO[R, Unit])
          env <- ZIO.environment[R]
          res <- producer.produceAsync(record)
        } yield res
          .tapCause(e => once(onError(record.topic, e)).provide(env))
          .tap(r => once(onSuccess(r)).provide(env))
      }

      override def shutdown: UIO[Unit] = producer.shutdown

      override def attributes: Map[String, String] = producer.attributes
    }
  }
}

case class ProducerConfig(bootstrapServers: String,
                          retryPolicy: ProducerRetryPolicy = ProducerRetryPolicy.Default,
                          extraProperties: Map[String, String] = Map.empty,
                          onProduceListener: ProducerRecord[_, _] => UIO[Unit] = _ => ZIO.unit) extends CommonGreyhoundConfig {
  def withBootstrapServers(servers: String) = copy(bootstrapServers = servers)

  def withRetryPolicy(retryPolicy: ProducerRetryPolicy) = copy(retryPolicy = retryPolicy)

  def withProperties(extraProperties: Map[String, String]) = copy(extraProperties = extraProperties)


  override def kafkaProps: Map[String, String] = Map(
    (KafkaProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
    (KafkaProducerConfig.RETRIES_CONFIG, retryPolicy.retries.toString),
    (KafkaProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryPolicy.backoff.toMillis.toString),
    (KafkaProducerConfig.ACKS_CONFIG, "all")
  ) ++ extraProperties
}

case class ProducerRetryPolicy(retries: Int, backoff: Duration)

object ProducerRetryPolicy {
  val Default = ProducerRetryPolicy(30, 200.millis)
}
