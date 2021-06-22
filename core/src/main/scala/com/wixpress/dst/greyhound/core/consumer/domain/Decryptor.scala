package com.wixpress.dst.greyhound.core.consumer.domain

import zio.{UIO, ZIO}

trait Decryptor[R, +E, K, V] { self =>
  def decrypt(record: ConsumerRecord[K, V]): ZIO[R, E, ConsumerRecord[K, V]]
  def provide(env: R) = new Decryptor[Any, E, K, V] {
    override def decrypt(record: ConsumerRecord[K, V]): ZIO[Any, E, ConsumerRecord[K, V]] = self.decrypt(record).provide(env)
  }
  def mapError[E1](f: E => E1)  = new Decryptor[R, E1, K, V] {
    override def decrypt(record: ConsumerRecord[K, V]): ZIO[R, E1, ConsumerRecord[K, V]] =
      self.decrypt(record).mapError(f)
  }
}

class NoOpDecryptor[R, +E, K, V] extends Decryptor[R, E, K, V]  {
  def decrypt(record: ConsumerRecord[K, V]): ZIO[R, E, ConsumerRecord[K, V]] = UIO(record)
}
