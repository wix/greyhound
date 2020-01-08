package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.producer._
import zio._

case class FakeProducer(records: Queue[ProducerRecord[Chunk[Byte], Chunk[Byte]]],
                        offset: Ref[Long],
                        config: ProducerConfig) extends Producer {

  override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): IO[ProducerError, RecordMetadata] =
    config match {
      case ProducerConfig.Standard =>
        for {
          offset <- offset.update(_ + 1)
          _ <- records.offer(record)
        } yield RecordMetadata(record.topic, record.partition.getOrElse(0), offset)

      case ProducerConfig.Failing =>
        ProducerError(new IllegalStateException("Oops"))
    }

  def failing: FakeProducer = copy(config = ProducerConfig.Failing)

}

object FakeProducer {
  def make: UIO[FakeProducer] = for {
    records <- Queue.unbounded[ProducerRecord[Chunk[Byte], Chunk[Byte]]]
    offset <- Ref.make(0L)
  } yield FakeProducer(records, offset, ProducerConfig.Standard)
}

sealed trait ProducerConfig

object ProducerConfig {
  case object Standard extends ProducerConfig
  case object Failing extends ProducerConfig
}
