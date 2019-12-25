package com.wixpress.dst.greyhound.core.consumer

import java.time
import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core._
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
  type Handler = RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]]

  private val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)

  private val topicPattern = """-retry-(\d+)$""".r.unanchored

  private val longDeserializer =
    Deserializer(new StringDeserializer).mapM(string => Task(string.toLong))

  private val instantDeserializer =
    longDeserializer.map(Instant.ofEpochMilli)

  private val durationDeserializer =
    longDeserializer.map(Duration(_, MILLISECONDS))

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
                              keyDeserializer: Deserializer[K],
                              valueDeserializer: Deserializer[V],
                              backoffs: Vector[Duration],
                              producer: Producer,
                              parallelism: Int = 8): URIO[R with Clock with GreyhoundMetrics, ConsumerSpec] =
    ZIO.access[R with Clock with GreyhoundMetrics] { r =>
      val handler2 = handler
        .mapError(RetryUserError[E])
        .contramapM(deserialize(keyDeserializer, valueDeserializer))

      val handler3 = RecordHandler[R with Clock with GreyhoundMetrics, RetryHandlerError[E], Chunk[Byte], Chunk[Byte]] { record =>
        retryAttemptOf(record.topic, record.headers).flatMap { retryAttempt =>
          val nextRetryAttempt = retryAttempt.fold(0)(_.attempt + 1)
          backoff(retryAttempt) *> handler2.handle(record).catchAll {
            case RetryUserError(_) if nextRetryAttempt < backoffs.length =>
              retryRecord(topic.name, group, backoffs, nextRetryAttempt, record)
                .flatMap(producer.produce)
                .mapError(RetryProducerError)

            case error => ZIO.fail(error)
          }
        }
      }

      ConsumerSpec(
        topics = retryTopics(topic.name, group, backoffs),
        group = group,
        handler = handler3
          .withErrorHandler(Metrics.report)
          .provide(r),
        parallelism = parallelism)
    }

  private def retryTopics(topic: TopicName, group: GroupName, backoffs: Vector[Duration]): Set[TopicName] =
    backoffs.indices.foldLeft(Set(topic))((acc, index) => acc + s"$topic-$group-retry-$index")

  private def backoff(retryAttempt: Option[RetryAttempt]): URIO[Clock, Unit] =
    ZIO.foreach_(retryAttempt) { attempt =>
      currentTime.flatMap { now =>
        val sleep = time.Duration.between(now, attempt.expiresAt)
        clock.sleep(Duration.fromJava(sleep))
      }
    }

  // TODO this is Wix specific - extract
  private def retryAttemptOf(topic: TopicName, headers: Headers): IO[RetrySerializationError, Option[RetryAttempt]] = {
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
    }.mapError(RetrySerializationError)
  }

  // TODO this is Wix specific - extract
  def retryRecord(topic: TopicName,
                  group: GroupName,
                  backoffs: Vector[Duration],
                  nextRetryAttempt: Int,
                  record: Record[Chunk[Byte], Chunk[Byte]]): URIO[Clock, ProducerRecord[Chunk[Byte], Chunk[Byte]]] =
    currentTime.map { now =>
      ProducerRecord(
        topic = Topic(s"$topic-$group-retry-$nextRetryAttempt"),
        value = record.value,
        key = record.key,
        partition = None,
        headers = record.headers +
          (RetryAttemptHeader.Submitted -> toChunk(now.toEpochMilli)) +
          (RetryAttemptHeader.Backoff -> toChunk(backoffs(nextRetryAttempt).toMillis)))
    }

  def toChunk(long: Long): Chunk[Byte] = Chunk.fromArray(long.toString.getBytes)

  private def deserialize[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V])
                               (record: Record[Chunk[Byte], Chunk[Byte]]): IO[RetryHandlerError[Nothing], Record[K, V]] =
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

// TODO this is Wix specific - extract
object RetryAttemptHeader {
  val Submitted = "submitTimestamp"
  val Backoff = "backOffTimeMs"
}
