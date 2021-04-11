package com.wixpress.dst.greyhound.core.consumer.domain

import zio.{UIO, ZIO}

trait Decryptor[R, +E, K, V] {
  def decrypt(record: ConsumerRecord[K, V]): ZIO[R, E, ConsumerRecord[K, V]]
}

class NoOpDecryptor[R, +E, K, V] extends Decryptor[R, E, K, V]  {
  def decrypt(record: ConsumerRecord[K, V]): ZIO[R, E, ConsumerRecord[K, V]] = UIO(record)
}
