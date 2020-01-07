package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.producer._
import zio._

case class FakeProducer(records: Queue[ProducerRecord[Chunk[Byte], Chunk[Byte]]],
                        config: ProducerConfig) extends Producer {

  override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): IO[ProducerError, RecordMetadata] =
    config match {
      case ProducerConfig.Standard =>
        records.offer(record).as(RecordMetadata(record.topic, 0, 0))

      case ProducerConfig.Failing =>
        ProducerError(new IllegalStateException("Oops"))
    }

  def failing: FakeProducer = copy(config = ProducerConfig.Failing)

}

object FakeProducer {
  def make: UIO[FakeProducer] =
    Queue.unbounded[ProducerRecord[Chunk[Byte], Chunk[Byte]]]
      .map(FakeProducer(_, ProducerConfig.Standard))
}

sealed trait ProducerConfig

object ProducerConfig {
  case object Standard extends ProducerConfig
  case object Failing extends ProducerConfig
}
