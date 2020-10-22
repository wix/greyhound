package com.wixpress.dst.greyhound.core.consumer.domain

import zio.ZIO

trait Decryptor[E, K, V] {
  def decrypt(record: ConsumerRecord[K, V]): ZIO[Any, E, ConsumerRecord[K, V]]
}
