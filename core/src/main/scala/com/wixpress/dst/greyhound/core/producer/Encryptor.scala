package com.wixpress.dst.greyhound.core.producer

import zio.{Chunk, Task, UIO}

trait Encryptor {
  def encrypt[K](record: ProducerRecord[K,Chunk[Byte]]): Task[ProducerRecord[K,Chunk[Byte]]]
}

case object NoOpEncryptor extends Encryptor {
  override def encrypt[K](record: ProducerRecord[K,Chunk[Byte]]): Task[ProducerRecord[K,Chunk[Byte]]] = UIO(record)
}
