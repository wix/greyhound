package com.wixpress.dst.greyhound.core.consumer.domain

import zio.ZIO

trait Decryptor[R, +E, K, V] {
  def decrypt(record: ConsumerRecord[K, V]): ZIO[R, E, ConsumerRecord[K, V]]
}
