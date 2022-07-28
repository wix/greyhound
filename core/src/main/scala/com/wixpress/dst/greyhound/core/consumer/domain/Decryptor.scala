package com.wixpress.dst.greyhound.core.consumer.domain

import zio.{Tag, Trace, ZEnvironment, ZIO, ZLayer}

trait Decryptor[R, +E, K, V] { self =>
  def decrypt(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[R, E, ConsumerRecord[K, V]]

  def provideEnvironment(env: ZEnvironment[R]) = new Decryptor[Any, E, K ,V] {
    override def decrypt(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[Any, E, ConsumerRecord[K, V]] =
      self.decrypt(record).provideEnvironment(env)
  }

  def provide[R1 <: R: Tag](env: R1)(implicit trace: Trace)          = new Decryptor[Any, E, K, V] {
    override def decrypt(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[Any, E, ConsumerRecord[K, V]] =
      self.decrypt(record).provideLayer(ZLayer.succeed(env))
  }
  def mapError[E1](f: E => E1) = new Decryptor[R, E1, K, V] {
    override def decrypt(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[R, E1, ConsumerRecord[K, V]] =
      self.decrypt(record).mapError(f)
  }
}

class NoOpDecryptor[R, +E, K, V] extends Decryptor[R, E, K, V] {
  def decrypt(record: ConsumerRecord[K, V])(implicit trace: Trace): ZIO[R, E, ConsumerRecord[K, V]] = ZIO.succeed(record)
}
