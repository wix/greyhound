package com.wixpress.dst.greyhound.core.producer

import zio.{Chunk, Task, Trace, ZIO}

trait Encryptor {
  def encrypt[K](record: ProducerRecord[K, Chunk[Byte]])(implicit trace: Trace): Task[ProducerRecord[K, Chunk[Byte]]]
}

case object NoOpEncryptor extends Encryptor {
  override def encrypt[K](record: ProducerRecord[K, Chunk[Byte]])(implicit trace: Trace): Task[ProducerRecord[K, Chunk[Byte]]] =
    ZIO.succeed(record)
}
