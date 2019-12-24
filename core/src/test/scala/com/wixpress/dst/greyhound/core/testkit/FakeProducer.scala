package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.producer._
import zio._

case class FakeProducer(records: Queue[ProducerRecord[Chunk[Byte], Chunk[Byte]]]) extends Producer {
  override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): IO[ProducerError, RecordMetadata] =
    records.offer(record).as(RecordMetadata(record.topic.name, 0, 0))
}

object FakeProducer {
  def make: UIO[FakeProducer] =
    Queue.unbounded[ProducerRecord[Chunk[Byte], Chunk[Byte]]].map(FakeProducer(_))
}
