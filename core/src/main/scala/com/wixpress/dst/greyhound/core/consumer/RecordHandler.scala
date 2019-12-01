package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Record
import zio.ZIO

trait RecordHandler[-R, +E, K, V] {
  def handle(record: Record[K, V]): ZIO[R, E, _]

  def contramapM[R1 <: R, E1 >: E, K1, V1](f: Record[K1, V1] => ZIO[R1, E1, Record[K, V]]): RecordHandler[R1, E1, K1, V1] =
    (record: Record[K1, V1]) => f(record).flatMap(handle)

  def withErrorHandler[R1 <: R, E1](f: E => ZIO[R1, E1, _]): RecordHandler[R1, E1, K, V] =
    (record: Record[K, V]) => handle(record).catchAll[R1, E1, Any](f)

  def provide(r: R): RecordHandler[Any, E, K, V] =
    (record: Record[K, V]) => handle(record).provide(r)

  // TODO test
  def filter(f: Record[K, V] => Boolean): RecordHandler[R, E, K, V] =
    (record: Record[K, V]) => ZIO.when(f(record))(handle(record))

  def par[R1 <: R, E1 >: E](other: RecordHandler[R1, E1, K, V]): RecordHandler[R1, E1, K, V] =
    (record: Record[K, V]) => handle(record) &> other.handle(record)

  def *>[R1 <: R, E1 >: E](other: RecordHandler[R1, E1, K, V]): RecordHandler[R1, E1, K, V] =
    (record: Record[K, V]) => handle(record) *> other.handle(record)
}

object RecordHandler {
  def apply[R, E, K, V](f: Record[K, V] => ZIO[R, E, _]): RecordHandler[R, E, K, V] =
    (record: Record[K, V]) => f(record)
}
