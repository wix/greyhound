package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.Consumer.{Key, Value}
import com.wixpress.dst.greyhound.core.consumer.ConsumerSpec.Handler
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import zio.clock.Clock
import zio.duration.Duration
import zio.{Task, URIO, ZIO, clock}

case class ConsumerSpec(topics: Set[TopicName],
                        group: GroupName,
                        handler: Handler,
                        parallelism: Int)

object ConsumerSpec {
  type Handler = RecordHandler[Any, Nothing, Key, Value]

  def make[R, E, K, V](topic: Topic[K, V],
                       group: GroupName,
                       handler: RecordHandler[R, E, K, V],
                       keyDeserializer: Deserializer[K],
                       valueDeserializer: Deserializer[V],
                       parallelism: Int = 8): URIO[R, ConsumerSpec] =
    ZIO.access[R] { r =>
      ConsumerSpec(
        topics = Set(topic.name),
        group = group,
        handler = handler
          .contramapM(deserialize(keyDeserializer, valueDeserializer))
          .withErrorHandler(e => ZIO.unit) // TODO report serialization errors
          .provide(r),
        parallelism = parallelism)
    }

  private val retryAttemptPattern = """-retry-(\d+)$""".r

  private val serializer = Serializer(new ByteArraySerializer)

  def makeWithRetry[R, K, V](topic: Topic[K, V],
                             group: GroupName,
                             handler: RecordHandler[R, Throwable, K, V],
                             keyDeserializer: Deserializer[K],
                             valueDeserializer: Deserializer[V],
                             retryPolicy: Vector[Duration],
                             producer: Producer,
                             parallelism: Int = 8): URIO[R with Clock, ConsumerSpec] =
    ZIO.access[R with Clock] { r =>
      val handlerWithRetry =
        handler
          .contramapM(deserialize(keyDeserializer, valueDeserializer))
          .flatMapError { e =>
            record: Record[Key, Value] =>
              // TODO add visibility
              // TODO smart sleep
              val retryAttempt = record.topic match {
                case retryAttemptPattern(attempt) => attempt.toInt
                case _ => 0
              }
              val nextRetryAttempt = retryAttempt + 1
              if (nextRetryAttempt < retryPolicy.length) {
                val retryTopic = Topic(s"${topic.name}-retry-$nextRetryAttempt")
                val backoff = retryPolicy(nextRetryAttempt)
                val producerRecord = ProducerRecord(retryTopic, record.value, record.key, headers = record.headers)
                clock.sleep(backoff) *> producer.produce(producerRecord, serializer, serializer)
              } else {
                ZIO.fail(e)
              }
          }
          .withErrorHandler(e => ZIO.unit) // TODO report serialization errors
          .provide(r)

      ConsumerSpec(
        topics = (1 to retryPolicy.length).foldLeft(Set(topic.name)) { (acc, retryAttempt) =>
          acc + s"${topic.name}-retry-$retryAttempt"
        },
        group = group,
        handler = handlerWithRetry,
        parallelism = parallelism)
    }

  def deserialize[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V])
                       (record: Record[Consumer.Key, Consumer.Value]): Task[Record[K, V]] = for {
    key <- ZIO.foreach(record.key)(keyDeserializer.deserialize(record.topic, record.headers, _))
    value <- valueDeserializer.deserialize(record.topic, record.headers, record.value)
  } yield Record(
    topic = record.topic,
    partition = record.partition,
    offset = record.offset,
    headers = record.headers,
    key = key.headOption,
    value = value)

}
