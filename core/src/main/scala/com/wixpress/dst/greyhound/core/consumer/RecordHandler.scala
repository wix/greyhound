package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Record
import zio.ZIO

trait RecordHandler[-R, +E, K, V] {
  def handle(record: Record[K, V]): ZIO[R, E, Any]

  def contramap[K2, V2](f: Record[K2, V2] => Record[K, V]): RecordHandler[R, E, K2, V2] =
    contramapM(record => ZIO.succeed(f(record)))

  def contramapM[R1 <: R, E1 >: E, K2, V2](f: Record[K2, V2] => ZIO[R1, E1, Record[K, V]]): RecordHandler[R1, E1, K2, V2] =
    (record: Record[K2, V2]) => f(record).flatMap(handle)

  def mapError[E2](f: E => E2): RecordHandler[R, E2, K, V] =
    (record: Record[K, V]) => handle(record).mapError(f)

  def flatMapError[R1 <: R, E2](f: E => RecordHandler[R1, E2, K, V]): RecordHandler[R1, E2, K, V] =
    (record: Record[K, V]) => handle(record).catchAll(e => f(e).handle(record))

  def withErrorHandler[R1 <: R, E2](f: E => ZIO[R1, E2, Any]): RecordHandler[R1, E2, K, V] =
    (record: Record[K, V]) => handle(record).catchAll(f)

  def provide(r: R): RecordHandler[Any, E, K, V] =
    (record: Record[K, V]) => handle(record).provide(r)

  def par[R1 <: R, E1 >: E](other: RecordHandler[R1, E1, K, V]): RecordHandler[R1, E1, K, V] =
    (record: Record[K, V]) => handle(record) &> other.handle(record)

  def *>[R1 <: R, E1 >: E](other: RecordHandler[R1, E1, K, V]): RecordHandler[R1, E1, K, V] =
    (record: Record[K, V]) => handle(record) *> other.handle(record)
}

object RecordHandler {
  def apply[R, E, K, V](f: Record[K, V] => ZIO[R, E, Any]): RecordHandler[R, E, K, V] =
    (record: Record[K, V]) => f(record)
}
