package com.wixpress.dst.greyhound.core.consumer.domain

import com.wixpress.dst.greyhound.core._
import zio._

/**
 * A `RecordHandler[R, E, K, V]` describes a handling function on one or more topics.
 * It handles records of type `ConsumerRecord[K, V]`, requires an environment of type `R`,
 * and might fail with errors of type `E`.
 */
trait RecordHandler[-R, +E, K, V] {
  self =>
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
      override def handle(record: ConsumerRecord[K2, V2]): ZIO[R1, E1, Any] =
        f(record).flatMap(self.handle)
    }

  /**
   * Return a handler which transforms the error type.
   */
  def mapError[E2](f: E => E2): RecordHandler[R, E2, K, V] =
    new RecordHandler[R, E2, K, V] {
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
    (record: ConsumerRecord[K, V]) => self.handle(record).catchAll(e => f(e, record))

  /**
    * Return a handler which can handle errors as [[Cause[E]]] (which includes [[Cause.Die]] and fiber traces), potentially eliminating them.
    *
    * {{{
    *   val failingHandler: RecordHandler[Any, Throwable, Int, String] = ???
    *
    *   val recoveringHandler: RecordHandler[Console, Nothing, Int, String] =
    *     failingHandler.withCauseHandler { cause =>
    *       zio.console.putStrLn(cause.squashTrace)
    *     }
    * }}}
    */
  def withErrorCauseHandler[R1 <: R, E2](f: (Cause[E], ConsumerRecord[K, V]) => ZIO[R1, E2, Any]): RecordHandler[R1, E2, K, V] =
    (record: ConsumerRecord[K, V]) => self.handle(record).catchAllCause(e => f(e, record))


  /**
   * Recover from all errors by ignoring them.
   */
  def ignore: RecordHandler[R, Nothing, K, V] =
    withErrorHandler((_, _) => ZIO.unit)

  /**
   * Provides the handler with its required environment, which eliminates
   * its dependency on `R`.
   */
  def provide(r: R): RecordHandler[Any, E, K, V] =
    new RecordHandler[Any, E, K, V] {
      override def handle(record: ConsumerRecord[K, V]): ZIO[Any, E, Any] =
        self.handle(record).provide(r)
    }

  /**
   * Invoke another action after this handler finishes.
   */
  def andThen[R1 <: R, E1 >: E](f: ConsumerRecord[K, V] => ZIO[R1, E1, Any]): RecordHandler[R1, E1, K, V] =
    new RecordHandler[R1, E1, K, V] {
      override def handle(record: ConsumerRecord[K, V]): ZIO[R1, E1, Any] =
        self.handle(record) *> f(record)
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
        value => Option(value).fold(Task[V](null.asInstanceOf[V]))(v => valueDeserializer.deserialize(record.topic, record.headers, v))
      ).mapError(e => Left(SerializationError(e)))
    }

  def withDecryptor[E1 >: E](dec: Decryptor[E1, K, V]) = {
    new RecordHandler[R, E1, K, V] {
      override def handle(record: ConsumerRecord[K, V]) =
        dec.decrypt(record).flatMap(self.handle)
    }
  }

}

case class SerializationError(cause: Throwable) extends RuntimeException(cause)

object RecordHandler {
  def apply[R, E, K, V](f: ConsumerRecord[K, V] => ZIO[R, E, Any]): RecordHandler[R, E, K, V] = {
    new RecordHandler[R, E, K, V] {
      override def handle(record: ConsumerRecord[K, V]): ZIO[R, E, Any] = f(record)
    }
  }

  def empty[K, V]: RecordHandler[Any, Nothing, K, V] =
    new RecordHandler[Any, Nothing, K, V] {
      override def handle(record: ConsumerRecord[K, V]): UIO[Any] = ZIO.unit
    }
}
