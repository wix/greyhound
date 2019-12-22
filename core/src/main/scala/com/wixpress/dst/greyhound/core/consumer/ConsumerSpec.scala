package com.wixpress.dst.greyhound.core.consumer

import java.time
import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.Consumer.{Key, Value}
import com.wixpress.dst.greyhound.core.consumer.ConsumerSpec.Handler
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerError, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import zio._
import zio.clock.Clock
import zio.duration._

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
                       parallelism: Int = 8): URIO[R with GreyhoundMetrics, ConsumerSpec] =
    ZIO.access[R with GreyhoundMetrics] { r =>
      ConsumerSpec(
        topics = Set(topic.name),
        group = group,
        handler = handler
          .mapError(RetryUserError(_))
          .contramapM(deserialize(keyDeserializer, valueDeserializer))
          .withErrorHandler(Metrics.report)
          .provide(r),
        parallelism = parallelism)
    }

  def withRetries[R, E, K, V](topic: Topic[K, V],
                              group: GroupName,
                              handler: RecordHandler[R, E, K, V],
                              keySerde: Serde[K],
                              valueSerde: Serde[V],
                              backoffs: Vector[Duration],
                              producer: Producer,
                              parallelism: Int = 8): URIO[R with Clock with GreyhoundMetrics, ConsumerSpec] =
    ZIO.access[R with Clock with GreyhoundMetrics] { r =>
      val handler1 = handler.contramap[K, (Option[RetryAttempt], V)](_.mapValue(_._2))
      val retryHandler = BackoffHandler[K, V] *> handler1.flatMapError { error =>
        RetryErrorHandler(error, topic, group, backoffs, producer, keySerde, valueSerde)
      }

      ConsumerSpec(
        topics = retryTopics(topic.name, group, backoffs),
        group = group,
        handler = retryHandler
          .contramapM(deserialize(keySerde, RetryAttemptDeserializer zip valueSerde))
          .withErrorHandler(Metrics.report)
          .provide(r),
        parallelism = parallelism)
    }

  private def retryTopics(topic: TopicName, group: GroupName, backoffs: Vector[Duration]): Set[TopicName] =
    backoffs.indices.foldLeft(Set(topic))((acc, index) => acc + s"$topic-$group-retry-$index")

  private def deserialize[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V])
                               (record: Record[Consumer.Key, Consumer.Value]): IO[RetryHandlerError[Nothing], Record[K, V]] =
    (for {
      key <- ZIO.foreach(record.key)(keyDeserializer.deserialize(record.topic, record.headers, _))
      value <- valueDeserializer.deserialize(record.topic, record.headers, record.value)
    } yield Record(
      topic = record.topic,
      partition = record.partition,
      offset = record.offset,
      headers = record.headers,
      key = key.headOption,
      value = value)).mapError(RetrySerializationError)
}

case class RetryAttempt(attempt: Int,
                        submittedAt: Instant,
                        backoff: Duration) {

  def expiresAt: Instant =
    submittedAt.plusMillis(backoff.toMillis)

}

sealed abstract class RetryHandlerError[+E](cause: Throwable = null)
  extends RuntimeException(cause) with GreyhoundMetric

case class RetrySerializationError(cause: Throwable) extends RetryHandlerError[Nothing](cause)
case class RetryProducerError(cause: ProducerError) extends RetryHandlerError[Nothing](cause)
case class RetryUserError[E](cause: E) extends RetryHandlerError[E]

// ---------------------------------------------------------------------------------

object BackoffHandler {
  private val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)

  def apply[K, V]: RecordHandler[Clock, Nothing, K, (Option[RetryAttempt], V)] =
    RecordHandler { record =>
      val (retryAttempt, _) = record.value
      ZIO.foreach_(retryAttempt) { attempt =>
        currentTime.flatMap { now =>
          val sleep = time.Duration.between(now, attempt.expiresAt)
          clock.sleep(Duration.fromJava(sleep))
        }
      }
    }
}

object RetryAttemptHeader {
  val Submitted = "submitTimestamp"
  val Backoff = "backOffTimeMs"
}

// TODO this is wix specific
object RetryAttemptDeserializer extends Deserializer[Option[RetryAttempt]] {
  private val topicPattern = """-retry-(\d+)$""".r.unanchored

  private val longDeserializer =
    Deserializer(new StringDeserializer).flatMap { string =>
      Deserializer(Task(string.toLong))
    }

  private val instantDeserializer =
    longDeserializer.map(Instant.ofEpochMilli)

  private val durationDeserializer =
    longDeserializer.map(Duration(_, MILLISECONDS))

  override def deserialize(topic: TopicName, headers: Headers, data: Chunk[Byte]): Task[Option[RetryAttempt]] = {
    val submittedHeader = headers.get(RetryAttemptHeader.Submitted, instantDeserializer)
    val backoffHeader = headers.get(RetryAttemptHeader.Backoff, durationDeserializer)
    (submittedHeader zipWith backoffHeader) { (submitted, backoff) =>
      val attempt = topic match {
        case topicPattern(attempt) => Some(attempt.toInt)
        case _ => None
      }
      for {
        a <- attempt
        s <- submitted
        b <- backoff
      } yield RetryAttempt(a, s, b)
    }
  }
}

// TODO this is wix specific
object RetryErrorHandler {
  def apply[E, K, V](originalError: E,
                     topic: Topic[K, V],
                     group: GroupName,
                     backoffs: Vector[Duration],
                     producer: Producer,
                     keySerializer: Serializer[K],
                     valueSerializer: Serializer[V]): RecordHandler[Clock, RetryHandlerError[E], K, (Option[RetryAttempt], V)] =
    RecordHandler { record =>
      val (retryAttempt, value) = record.value
      val nextRetryAttempt = retryAttempt.fold(0)(_.attempt + 1)
      if (nextRetryAttempt < backoffs.length) {
        clock.currentTime(MILLISECONDS).flatMap { now =>
          producer.produce(
            record = ProducerRecord(
              topic = Topic(s"${topic.name}-$group-retry-$nextRetryAttempt"),
              value = value,
              key = record.key,
              partition = None,
              headers = record.headers +
                (RetryAttemptHeader.Submitted -> Chunk.fromArray(now.toString.getBytes)) +
                (RetryAttemptHeader.Backoff -> Chunk.fromArray(backoffs(nextRetryAttempt).toMillis.toString.getBytes))),
            keySerializer = keySerializer,
            valueSerializer = valueSerializer).mapError(RetryProducerError)
        }
      } else {
        ZIO.fail(RetryUserError(originalError))
      }
    }
}
