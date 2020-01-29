package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerError}
import zio._
import zio.clock.Clock

trait RecordHandler[-R, +E, K, V] { self =>
  def topics: Set[Topic]

  def handle(record: ConsumerRecord[K, V]): ZIO[R, E, Unit]

  def contramap[K2, V2](f: ConsumerRecord[K2, V2] => ConsumerRecord[K, V]): RecordHandler[R, E, K2, V2] =
    contramapM(record => ZIO.succeed(f(record)))

  def contramapM[R1 <: R, E1 >: E, K2, V2](f: ConsumerRecord[K2, V2] => ZIO[R1, E1, ConsumerRecord[K, V]]): RecordHandler[R1, E1, K2, V2] =
    new RecordHandler[R1, E1, K2, V2] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K2, V2]): ZIO[R1, E1, Unit] =
        f(record).flatMap(self.handle)
    }

  def mapError[E2](f: E => E2): RecordHandler[R, E2, K, V] =
    new RecordHandler[R, E2, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[R, E2, Unit] =
        self.handle(record).mapError(f)
    }

  def withErrorHandler[R1 <: R, E2](f: E => ZIO[R1, E2, Unit]): RecordHandler[R1, E2, K, V] =
    new RecordHandler[R1, E2, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[R1, E2, Unit] =
        self.handle(record).catchAll(f)
    }

  def ignore: RecordHandler[R, Nothing, K, V] =
    withErrorHandler(_ => ZIO.unit)

  def provide(r: R): RecordHandler[Any, E, K, V] =
    new RecordHandler[Any, E, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[Any, E, Unit] =
        self.handle(record).provide(r)
    }

  def andThen[R1 <: R, E1 >: E](f: ConsumerRecord[K, V] => ZIO[R1, E1, Unit]): RecordHandler[R1, E1, K, V] =
    new RecordHandler[R1, E1, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[R1, E1, Unit] =
        self.handle(record) *> f(record)
    }

  def combine[R1 <: R, E1 >: E](other: RecordHandler[R1, E1, K, V]): RecordHandler[R1, E1, K, V] =
    new RecordHandler[R1, E1, K, V] {
      type Handler = ConsumerRecord[K, V] => ZIO[R1, E1, Unit]

      private val handlerByTopic: Map[Topic, Handler] =
        List(self, other).foldLeft(Map.empty[Topic, Handler]) { (acc, handler) =>
          handler.topics.foldLeft(acc) { (acc1, topic) =>
            val newHandler = acc1.get(topic).fold[Handler](handler.handle) { oldHandler =>
              record => oldHandler(record) zipParRight handler.handle(record)
            }
            acc1 + (topic -> newHandler)
          }
        }

      override def topics: Set[Topic] = self.topics union other.topics

      override def handle(record: ConsumerRecord[K, V]): ZIO[R1, E1, Unit] =
        handlerByTopic.get(record.topic) match {
          case Some(handler) => handler(record)
          case None => ZIO.unit
        }
    }

  def withDeserializers(keyDeserializer: Deserializer[K],
                        valueDeserializer: Deserializer[V]): RecordHandler[R, Either[SerializationError, E], Chunk[Byte], Chunk[Byte]] =
    mapError(Right(_)).contramapM { record =>
      (for {
        key <- ZIO.foreach(record.key)(keyDeserializer.deserialize(record.topic, record.headers, _))
        value <- valueDeserializer.deserialize(record.topic, record.headers, record.value)
      } yield ConsumerRecord(
        topic = record.topic,
        partition = record.partition,
        offset = record.offset,
        headers = record.headers,
        key = key.headOption,
        value = value)).mapError(e => Left(SerializationError(e)))
    }

  def withRetries[R2, R3](retryPolicy: RetryPolicy[R2, E], producer: Producer[R3])
                         (implicit evK: K <:< Chunk[Byte], evV: V <:< Chunk[Byte]): RecordHandler[R with R2 with R3 with Clock, Either[ProducerError, E], K, V] =
    new RecordHandler[R with R2 with R3 with Clock, Either[ProducerError, E], K, V] {
      override def topics: Set[Topic] = for {
        originalTopic <- self.topics
        topic <- retryPolicy.retryTopics(originalTopic) + originalTopic
      } yield topic

      override def handle(record: ConsumerRecord[K, V]): ZIO[R with R2 with R3 with Clock, Either[ProducerError, E], Unit] =
        retryPolicy.retryAttempt(record.topic, record.headers).flatMap { retryAttempt =>
          ZIO.foreach_(retryAttempt)(_.sleep) *> self.handle(record).catchAll { e =>
            retryPolicy.retryDecision(retryAttempt, record.bimap(evK, evV), e).flatMap {
              case RetryWith(retryRecord) => producer.produce(retryRecord).unit.mapError(Left(_))
              case NoMoreRetries => ZIO.fail(Right(e))
            }
          }
        }
    }

}

case class SerializationError(cause: Throwable) extends RuntimeException(cause)

object RecordHandler {
  def apply[R, E, K, V](topics: Topic*)(f: ConsumerRecord[K, V] => ZIO[R, E, Unit]): RecordHandler[R, E, K, V] = {
    val topics1 = topics.toSet
    new RecordHandler[R, E, K, V] {
      override def topics: Set[Topic] = topics1
      override def handle(record: ConsumerRecord[K, V]): ZIO[R, E, Unit] = f(record)
    }
  }

  def empty[K, V]: RecordHandler[Any, Nothing, K, V] =
    new RecordHandler[Any, Nothing, K, V] {
      override def topics: Set[Topic] = Set.empty
      override def handle(record: ConsumerRecord[K, V]): UIO[Unit] = ZIO.unit
    }
}
