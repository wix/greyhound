package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig => KafkaProducerConfig, ProducerRecord => KafkaProducerRecord, RecordMetadata => KafkaRecordMetadata}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import zio._

import scala.collection.JavaConverters._
import zio.ZIO.attemptBlocking
import zio.managed._

trait ProducerR[-R] { self =>
  def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]])(
    implicit trace: Trace
  ): ZIO[R, ProducerError, IO[ProducerError, RecordMetadata]]

  def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]])(implicit trace: Trace): ZIO[R, ProducerError, RecordMetadata] =
    for {
      promise <- produceAsync(record)
      res     <- promise // promise is ZIO itself (the result of from Promise.await)
    } yield res

  def produce[K, V](
    record: ProducerRecord[K, V],
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    encryptor: Encryptor = NoOpEncryptor
  )(implicit trace: Trace): ZIO[R, ProducerError, RecordMetadata] =
    serialized(record, keySerializer, valueSerializer, encryptor)
      .mapError(SerializationError)
      .flatMap(produce)

  def produceAsync[K, V](
    record: ProducerRecord[K, V],
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    encryptor: Encryptor = NoOpEncryptor
  )(implicit trace: Trace): ZIO[R, ProducerError, IO[ProducerError, RecordMetadata]] =
    serialized(record, keySerializer, valueSerializer, encryptor)
      .mapError(SerializationError)
      .flatMap(produceAsync)

  def partitionsFor(topic: Topic)(implicit trace: Trace): RIO[R, Seq[PartitionInfo]]

  def shutdown(implicit trace: Trace): UIO[Unit] = ZIO.unit

  def attributes: Map[String, String] = Map.empty

  private def serialized[V, K](
    record: ProducerRecord[K, V],
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    encryptor: Encryptor
  )(implicit trace: Trace) = {
    for {
      keyBytes        <- keySerializer.serializeOpt(record.topic, record.key)
      valueBytes      <- valueSerializer.serializeOpt(record.topic, record.value)
      serializedRecord = ProducerRecord(
                           topic = record.topic,
                           value = valueBytes,
                           key = keyBytes,
                           partition = record.partition,
                           headers = record.headers
                         )
      encyptedRecord  <- encryptor.encrypt(serializedRecord)
    } yield encyptedRecord
  }
}

object Producer {
  private val serializer = new ByteArraySerializer
  type Producer = ProducerR[Any]

  def make(config: ProducerConfig, attrs: Map[String, String] = Map.empty)(implicit trace: Trace): RIO[Scope, Producer] =
    makeR[Any](config, attrs)

  def makeR[R](config: ProducerConfig, attrs: Map[String, String] = Map.empty)(implicit trace: Trace): RIO[Scope, ProducerR[R]] = {
    val acquire = ZIO.attemptBlocking(new KafkaProducer(config.properties, serializer, serializer))
    ZIO.acquireRelease(acquire)(producer => attemptBlocking(producer.close()).ignore).map { producer =>
      new ProducerR[R] {
        private def recordFrom(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) =
          new KafkaProducerRecord(
            record.topic,
            record.partition.fold[Integer](null)(Integer.valueOf),
            record.key.fold[Array[Byte]](null)(_.toArray),
            record.value.map(_.toArray).orNull,
            headersFrom(record.headers).asJava
          )

        private def headersFrom(headers: Headers): Iterable[Header] =
          headers.headers.map {
            case (key, value) =>
              new RecordHeader(key, value.toArray)
          }

        override def produceAsync(
          record: ProducerRecord[Chunk[Byte], Chunk[Byte]]
        )(implicit trace: Trace): ZIO[R, ProducerError, IO[ProducerError, RecordMetadata]] =
          for {
            produceCompletePromise <- Promise.make[ProducerError, RecordMetadata]
            runtime                <- ZIO.runtime[Any]
            _                      <- attemptBlocking(
                                        producer.send(
                                          recordFrom(record),
                                          new Callback {
                                            override def onCompletion(metadata: KafkaRecordMetadata, exception: Exception): Unit = {
                                              zio.Unsafe.unsafe { implicit s =>
                                                runtime.unsafe
                                                  .run(
                                                    (if (exception != null) produceCompletePromise.complete(ProducerError(exception))
                                                     else produceCompletePromise.succeed(RecordMetadata(metadata))) *> config.onProduceListener(record)
                                                  )
                                                  .getOrThrowFiberFailure()
                                              }
                                            }
                                          }
                                        )
                                      )
                                        .catchAll(e => produceCompletePromise.complete(ProducerError(e)))
          } yield (produceCompletePromise.await)

        override def attributes: Map[String, String] = attrs

        override def partitionsFor(topic: Topic)(implicit trace: Trace): RIO[Any, Seq[PartitionInfo]] = {
          attemptBlocking(
            producer
              .partitionsFor(topic)
              .asScala
              .toSeq
              .map(PartitionInfo.apply)
          )
        }
      }
    }
  }
}

object ProducerR {
  implicit class Ops[R <: Any](producer: ProducerR[R]) {
    // R1 is a work around to an apparent bug in Has.union ¯\_(ツ)_/¯
    // https://github.com/zio/zio/issues/3558#issuecomment-776051184
    def provide(env: ZEnvironment[R])                                            = new ProducerR[Any] {
      override def produceAsync(
        record: ProducerRecord[Chunk[Byte], Chunk[Byte]]
      )(implicit trace: Trace): ZIO[Any, ProducerError, IO[ProducerError, RecordMetadata]] =
        producer.produceAsync(record).provideEnvironment(env)

      override def attributes: Map[String, String] = producer.attributes

      override def shutdown(implicit trace: Trace): UIO[Unit] = producer.shutdown

      override def partitionsFor(topic: Topic)(implicit trace: Trace): RIO[Any, Seq[PartitionInfo]] =
        producer.partitionsFor(topic).provideEnvironment(env)
    }
    def onShutdown(onShutdown: => UIO[Unit])(implicit trace: Trace): ProducerR[R] = new ProducerR[R] {
      override def produceAsync(
        record: ProducerRecord[Chunk[Byte], Chunk[Byte]]
      )(implicit trace: Trace): ZIO[R, ProducerError, IO[ProducerError, RecordMetadata]] =
        producer.produceAsync(record)

      override def shutdown(implicit trace: Trace): UIO[Unit] = onShutdown *> producer.shutdown

      override def attributes: Map[String, String] = producer.attributes

      override def partitionsFor(topic: Topic)(implicit trace: Trace) = producer.partitionsFor(topic)
    }

    def tapBoth(onError: (Topic, Cause[ProducerError]) => URIO[R, Unit], onSuccess: RecordMetadata => URIO[R, Unit]) = new ProducerR[R] {
      override def produceAsync(
        record: ProducerRecord[Chunk[Byte], Chunk[Byte]]
      )(implicit trace: Trace): ZIO[R, ProducerError, IO[ProducerError, RecordMetadata]] = {
        for {
          called <- Ref.make(false)
          once    = ZIO.whenZIO(called.getAndUpdate(_ => true).negate)(_: URIO[R, Unit])
          env    <- ZIO.environment[R]
          res    <- producer.produceAsync(record)
        } yield res
          .tapErrorCause(e => once(onError(record.topic, e)).provideEnvironment(env))
          .tap(r => once(onSuccess(r)).provideEnvironment(env))
      }

      override def shutdown(implicit trace: Trace): UIO[Unit] = producer.shutdown

      override def attributes: Map[String, String] = producer.attributes

      override def partitionsFor(topic: Topic)(implicit trace: Trace) = producer.partitionsFor(topic)
    }

    def map(f: ProducerRecord[Chunk[Byte], Chunk[Byte]] => ProducerRecord[Chunk[Byte], Chunk[Byte]]) = new ProducerR[R] {
      override def produceAsync(
        record: ProducerRecord[Chunk[Byte], Chunk[Byte]]
      )(implicit trace: Trace): ZIO[R, ProducerError, IO[ProducerError, RecordMetadata]] =
        producer.produceAsync(f(record))

      override def partitionsFor(topic: Topic)(implicit trace: Trace): RIO[R, Seq[PartitionInfo]] = producer.partitionsFor(topic)

      override def shutdown(implicit trace: Trace): UIO[Unit] = producer.shutdown

      override def attributes: Map[String, String] = producer.attributes
    }
  }

}

case class ProducerConfig(
  bootstrapServers: String,
  retryPolicy: ProducerRetryPolicy = ProducerRetryPolicy.Default,
  extraProperties: Map[String, String] = Map.empty,
  onProduceListener: ProducerRecord[_, _] => UIO[Unit] = _ => ZIO.unit
) extends CommonGreyhoundConfig {
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
