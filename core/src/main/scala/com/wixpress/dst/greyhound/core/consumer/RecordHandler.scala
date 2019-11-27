package com.wixpress.dst.greyhound.core.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import zio.ZIO

trait RecordHandler[-R, +E, K, V] {
  def handle(record: ConsumerRecord[K, V]): ZIO[R, E, _]

  def contramapM[R1 <: R, E1 >: E, K1, V1](f: ConsumerRecord[K1, V1] => ZIO[R1, E1, ConsumerRecord[K, V]]): RecordHandler[R1, E1, K1, V1] =
    (record: ConsumerRecord[K1, V1]) => f(record).flatMap(handle)

  def withErrorHandler[R1 <: R, E1](f: E => ZIO[R1, E1, _]): RecordHandler[R1, E1, K, V] =
    (record: ConsumerRecord[K, V]) => handle(record).catchAll[R1, E1, Any](f)

  def provide(r: R): RecordHandler[Any, E, K, V] =
    (record: ConsumerRecord[K, V]) => handle(record).provide(r)

  // TODO test
  def filter(f: ConsumerRecord[K, V] => Boolean): RecordHandler[R, E, K, V] =
    (record: ConsumerRecord[K, V]) => if (f(record)) handle(record) else ZIO.unit
}

object RecordHandler {
  def apply[R, E, K, V](f: ConsumerRecord[K, V] => ZIO[R, E, _]): RecordHandler[R, E, K, V] =
    (record: ConsumerRecord[K, V]) => f(record)
}
