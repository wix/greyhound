package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.Offset
import com.wixpress.dst.greyhound.core.consumer.domain.TopicPartition
import com.wixpress.dst.greyhound.core.producer.Producer.Producer
import com.wixpress.dst.greyhound.core.producer._
import zio._
import zio.blocking.Blocking

case class FakeProducer(records: Queue[ProducerRecord[Chunk[Byte], Chunk[Byte]]],
                        offsets: Ref[Map[TopicPartition, Offset]],
                        config: ProducerConfig) extends Producer {

  def failing: FakeProducer = copy(config = ProducerConfig.Failing)

  override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, Promise[ProducerError, RecordMetadata]] =
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
          promise <- Promise.make[ProducerError, RecordMetadata].tap(_.succeed(RecordMetadata(topic, partition, offset)))
        } yield promise

      case ProducerConfig.Failing =>
        ProducerError(new IllegalStateException("Oops")).flip.flatMap(error =>
          Promise.make[ProducerError, RecordMetadata]
            .tap(_.fail(error)))
    }
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
