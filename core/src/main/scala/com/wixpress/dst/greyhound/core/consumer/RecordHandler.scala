package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.RetryDecision.{NoMoreRetries, RetryWith}
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerError}
import zio._
import zio.clock.Clock

/**
  * A `RecordHandler[R, E, K, V]` describes a handling function on one or more topics.
  * It handles records of type `ConsumerRecord[K, V]`, requires an environment of type `R`,
  * and might fail with errors of type `E`.
  */
trait RecordHandler[-R, +E, K, V] { self =>

  /**
    * Topics that this handler should consume.
    */
  def topics: Set[Topic]

  /**
    * Handle a single record.
    */
  def handle(record: ConsumerRecord[K, V]): ZIO[R, E, Any]

  /**
    * Returns a handler which adapts the input by using an pure function `f`.
    */
  def contramap[K2, V2](f: ConsumerRecord[K2, V2] => ConsumerRecord[K, V]): RecordHandler[R, E, K2, V2] =
    contramapM(record => ZIO.succeed(f(record)))

  /**
    * Returns a handler which adapts the input by using an effectful function `f`.
    *
    * {{{
    *   val personHandler: RecordHandler[Any, Nothing, PersonId, Person] = ???
    *
    *   def parsePersonId(personIdJson: String): IO[JsonParseError, PersonId] = ???
    *
    *   def parsePerson(personJson: String): IO[JsonParseError, Person] = ???
    *
    *   val jsonHandler: RecordHandler[Any, JsonParseError, String, String] =
    *     personHandler.contramapM { record =>
    *       record.bimapM(parsePersonId, parsePerson)
    *     }
    * }}}
    */
  def contramapM[R1 <: R, E1 >: E, K2, V2](f: ConsumerRecord[K2, V2] => ZIO[R1, E1, ConsumerRecord[K, V]]): RecordHandler[R1, E1, K2, V2] =
    new RecordHandler[R1, E1, K2, V2] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K2, V2]): ZIO[R1, E1, Any] =
        f(record).flatMap(self.handle)
    }

  /**
    * Return a handler which transforms the error type.
    */
  def mapError[E2](f: E => E2): RecordHandler[R, E2, K, V] =
    new RecordHandler[R, E2, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[R, E2, Any] =
        self.handle(record).mapError(f)
    }

  /**
    * Return a handler which can handle errors, potentially eliminating them.
    *
    * {{{
    *   val failingHandler: RecordHandler[Any, Throwable, Int, String] = ???
    *
    *   val recoveringHandler: RecordHandler[Console, Nothing, Int, String] =
    *     failingHandler.withErrorHandler { error =>
    *       zio.console.putStrLn(error.getMessage)
    *     }
    * }}}
    */
  def withErrorHandler[R1 <: R, E2](f: (E, ConsumerRecord[K, V]) => ZIO[R1, E2, Any]): RecordHandler[R1, E2, K, V] =
    new RecordHandler[R1, E2, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[R1, E2, Any] =
        self.handle(record).catchAll(e => f(e, record))
    }

  /**
    * Recover from all errors by ignoring them.
    */
  def ignore: RecordHandler[R, Nothing, K, V] =
    withErrorHandler((_,_) => ZIO.unit)

  /**
    * Provides the handler with its required environment, which eliminates
    * its dependency on `R`.
    */
  def provide(r: R): RecordHandler[Any, E, K, V] =
    new RecordHandler[Any, E, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[Any, E, Any] =
        self.handle(record).provide(r)
    }

  /**
    * Invoke another action after this handler finishes.
    */
  def andThen[R1 <: R, E1 >: E](f: ConsumerRecord[K, V] => ZIO[R1, E1, Any]): RecordHandler[R1, E1, K, V] =
    new RecordHandler[R1, E1, K, V] {
      override def topics: Set[Topic] = self.topics
      override def handle(record: ConsumerRecord[K, V]): ZIO[R1, E1, Any] =
        self.handle(record) *> f(record)
    }

  /**
    * Combine this handler with another handler. The combined handler will subscribe to
    * all topics from both handlers, and will invoke the correct handler according
    * to the incoming record's topic.
    *
    * NOTE: It is discouraged to combine 2 handlers that are subscribed to the same
    * topic, as they'll share the same consumer group. If you find yourself in this
    * situation, you probably need to create two distinct handlers with two groups.
    */
  def combine[R1 <: R, E1 >: E](other: RecordHandler[R1, E1, K, V]): RecordHandler[R1, E1, K, V] =
    new RecordHandler[R1, E1, K, V] {
      type Handler = ConsumerRecord[K, V] => ZIO[R1, E1, Any]

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

      override def handle(record: ConsumerRecord[K, V]): ZIO[R1, E1, Any] =
        handlerByTopic.get(record.topic) match {
          case Some(handler) => handler(record)
          case None => ZIO.unit
        }
    }

  /**
    * Return a handler which uses deserializers to adapt the input from
    * bytes to the needed types `K` and `V`.
    */
  def withDeserializers(keyDeserializer: Deserializer[K],
                        valueDeserializer: Deserializer[V]): RecordHandler[R, Either[SerializationError, E], Chunk[Byte], Chunk[Byte]] =
    mapError(Right(_)).contramapM { record =>
      record.bimapM(
        key => keyDeserializer.deserialize(record.topic, record.headers, key),
        value => valueDeserializer.deserialize(record.topic, record.headers, value)
      ).mapError(e => Left(SerializationError(e)))
    }

  /**
    * Return a handler with added retry behavior based on the provided `RetryPolicy`.
    * Upon failures, the `producer` will be used to send the failing records to designated
    * retry topics where the handling will be retried, after an optional delay. This
    * allows the handler to keep processing records in the original topic - however,
    * ordering will be lost for retried records!
    */
  def withRetries[R2, R3](retryPolicy: RetryPolicy[R2, E], producer: Producer[R3])
                         (implicit evK: K <:< Chunk[Byte], evV: V <:< Chunk[Byte]): RecordHandler[R with R2 with R3 with Clock, Either[ProducerError, E], K, V] =
    new RecordHandler[R with R2 with R3 with Clock, Either[ProducerError, E], K, V] {
      override def topics: Set[Topic] = for {
        originalTopic <- self.topics
        topic <- retryPolicy.retryTopics(originalTopic) + originalTopic
      } yield topic

      override def handle(record: ConsumerRecord[K, V]): ZIO[R with R2 with R3 with Clock, Either[ProducerError, E], Any] =
        retryPolicy.retryAttempt(record.topic, record.headers).flatMap { retryAttempt =>
          ZIO.foreach_(retryAttempt)(_.sleep) *> self.handle(record).catchAll { e =>
            retryPolicy.retryDecision(retryAttempt, record.bimap(evK, evV), e).flatMap {
              case RetryWith(retryRecord) => producer.produce(retryRecord).mapError(Left(_))
              case NoMoreRetries => ZIO.fail(Right(e))
            }
          }
        }
    }

}

case class SerializationError(cause: Throwable) extends RuntimeException(cause)

object RecordHandler {
  def apply[R, E, K, V](topics: Topic*)(f: ConsumerRecord[K, V] => ZIO[R, E, Any]): RecordHandler[R, E, K, V] = {
    val topics1 = topics.toSet
    new RecordHandler[R, E, K, V] {
      override def topics: Set[Topic] = topics1
      override def handle(record: ConsumerRecord[K, V]): ZIO[R, E, Any] = f(record)
    }
  }

  def empty[K, V]: RecordHandler[Any, Nothing, K, V] =
    new RecordHandler[Any, Nothing, K, V] {
      override def topics: Set[Topic] = Set.empty
      override def handle(record: ConsumerRecord[K, V]): UIO[Any] = ZIO.unit
    }
}
