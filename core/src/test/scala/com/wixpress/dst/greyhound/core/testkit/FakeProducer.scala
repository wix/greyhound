package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.Offset
import com.wixpress.dst.greyhound.core.consumer.TopicPartition
import com.wixpress.dst.greyhound.core.producer._
import zio._

case class FakeProducer(records: Queue[ProducerRecord[Chunk[Byte], Chunk[Byte]]],
                        offsets: Ref[Map[TopicPartition, Offset]],
                        config: ProducerConfig) extends Producer[Any] {

  override def produce(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): IO[ProducerError, RecordMetadata] =
    config match {
      case ProducerConfig.Standard =>
        for {
          _ <- records.offer(record)
          topic = record.topic
          partition = record.partition.getOrElse(0)
          topicPartition = TopicPartition(topic, partition)
          offset <- offsets.modify { offsets =>
            val offset = offsets.get(topicPartition).fold(0L)(_ + 1)
            (offset, offsets + (topicPartition -> offset))
          }
        } yield RecordMetadata(topic, partition, offset)

      case ProducerConfig.Failing =>
        ProducerError(new IllegalStateException("Oops"))
    }

  def failing: FakeProducer = copy(config = ProducerConfig.Failing)

}

object FakeProducer {
  def make: UIO[FakeProducer] = for {
    records <- Queue.unbounded[ProducerRecord[Chunk[Byte], Chunk[Byte]]]
    offset <- Ref.make(Map.empty[TopicPartition, Offset])
  } yield FakeProducer(records, offset, ProducerConfig.Standard)
}

sealed trait ProducerConfig

object ProducerConfig {
  case object Standard extends ProducerConfig
  case object Failing extends ProducerConfig
}
