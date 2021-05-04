package com.wixpress.dst.greyhound.core.consumer.domain

import com.wixpress.dst.greyhound.core.{Deserializer, Partition}
import zio.{Cause, Chunk, Task, ZIO}

case class ConsumerRecordBatch[K, V](topic: String, partition: Partition, records: Seq[ConsumerRecord[K, V]]) {
  def size = records.size
  def startOffset = records.headOption.fold(0L)(_.offset)
  def endOffset = records.lastOption.fold(0L)(_.offset)
}

trait BatchRecordHandler[-R, +E, K, V] { self =>
  def handle(records: ConsumerRecordBatch[K, V]): ZIO[R, HandleError[E], Any]

  /**
    * Invoke another action after this handler finishes.
    */
  def andThen[R1 <: R, E1 >: E](f:  ConsumerRecordBatch[K, V] => ZIO[R1, HandleError[E1], Any]): BatchRecordHandler[R1, E1, K, V] =
    (records: ConsumerRecordBatch[K, V]) => self.handle(records) *> f(records)

  /**
   * Invoke another action before this handler
   */
  def <*[R1 <: R, E1 >: E](f:  ConsumerRecordBatch[K, V] => ZIO[R1, HandleError[E1], Any]): BatchRecordHandler[R1, E1, K, V] = {
    (records: ConsumerRecordBatch[K, V]) => f(records) *> self.handle(records)
  }

  /**
    * returns a handler for which errors will be passed to the given function (for logging and such)
    */
  def tapError[R1 <: R](f: (HandleError[E],  ConsumerRecordBatch[K, V]) => ZIO[R1, Nothing, Any]): BatchRecordHandler[R1, E, K, V] =
    (records:  ConsumerRecordBatch[K, V]) => self.handle(records).tapError(e => f(e, records))

  /**
    * returns a handler for which errors/defects will be passed to the given function (for logging and such)
    */
  def tapCause[R1 <: R](f: (Cause[HandleError[E]],  ConsumerRecordBatch[K, V]) => ZIO[R1, Nothing, Any]): BatchRecordHandler[R1, E, K, V] =
    (records:  ConsumerRecordBatch[K, V]) => self.handle(records).tapCause(e => f(e, records))


  /**
    * Return a handler which transforms the error type.
    */
  def mapError[E2](f: E => E2): BatchRecordHandler[R, E2, K, V] =
    (records:  ConsumerRecordBatch[K, V]) => self.handle(records).mapError(e => e.mapError(f))

  /**
    * Returns a handler which adapts the input by using an effectful function `f`.
    */
  def contramapM[R1 <: R, E1 >: E, K2, V2](f: Seq[ConsumerRecord[K2, V2]] => ZIO[R1, HandleError[E1], Seq[ConsumerRecord[K, V]]]): BatchRecordHandler[R1, E1, K2, V2] =
    (records: ConsumerRecordBatch[K2, V2]) => f(records.records).flatMap(rs => self.handle(ConsumerRecordBatch(records.topic, records.partition, rs)))

  /**
    * Return a handler which uses deserializers to adapt the input from
    * bytes to the needed types `K` and `V`.
    */
  def withDeserializers(keyDeserializer: Deserializer[K],
                        valueDeserializer: Deserializer[V]): BatchRecordHandler[R, Either[SerializationError, E], Chunk[Byte], Chunk[Byte]] =
    mapError(Right(_)).contramapM { records =>
      ZIO.foreach(records) {
        record =>
          record.bimapM(
            key => keyDeserializer.deserialize(record.topic, record.headers, key),
            value => Option(value).fold(Task[V](null.asInstanceOf[V]))(
              v => valueDeserializer.deserialize(
                record.topic,
                record.headers,
                v))
          )
      }.mapError(e => HandleError(Left(SerializationError(e))))
    }

  def withDecryptor[E1 >: E, R1 <: R](dec: Decryptor[R1, E1, K, V]) = {
    new BatchRecordHandler[R1, E1, K, V] {
      override def handle(records: ConsumerRecordBatch[K, V]) =
        ZIO.foreach(records.records)(dec.decrypt)
          .mapError(e => HandleError(e))
          .flatMap(rs => self.handle(ConsumerRecordBatch(records.topic, records.partition, rs)))
    }
  }
}

object BatchRecordHandler {
  def apply[R, E, K, V] (handle: ConsumerRecordBatch[K, V] => ZIO[R, HandleError[E], Any]): BatchRecordHandler[R, E, K, V]  =
    recs =>  handle(recs)
}

case class HandleError[+E](error: E, forceNoRetry: Boolean = false) {
  def mapError[E1](f: E => E1) = HandleError[E1](f(error), forceNoRetry)
}
