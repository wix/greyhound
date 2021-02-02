package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.{Offset, TopicPartition}
import com.wixpress.dst.greyhound.core.producer.Producer.Producer
import com.wixpress.dst.greyhound.core.producer._
import zio._
import zio.blocking.Blocking

case class FakeProducer(records: Queue[ProducerRecord[Chunk[Byte], Chunk[Byte]]],
                        offsets: Ref[Map[TopicPartition, Offset]],
                        config: ProducerConfig,
                        beforeProduce: ProducerRecord[Chunk[Byte], Chunk[Byte]] => IO[ProducerError, ProducerRecord[Chunk[Byte], Chunk[Byte]]] = UIO(_),
                        beforeComplete: RecordMetadata => IO[ProducerError, RecordMetadata] = UIO(_),
                        override val attributes: Map[String, String] = Map.empty
                       ) extends Producer {

  def failing: FakeProducer = copy(config = ProducerConfig.Failing)

  override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, IO[ProducerError, RecordMetadata]] =
    config match {
      case ProducerConfig.Standard =>
        for {
          modified <- beforeProduce(record)
          _ <- records.offer(modified)
          topic = modified.topic
          partition = modified.partition.getOrElse(0)
          topicPartition = TopicPartition(topic, partition)
          offset <- offsets.modify { offsets =>
            val offset = offsets.get(topicPartition).fold(0L)(_ + 1)
            (offset, offsets + (topicPartition -> offset))
          }
          promise <- Promise.make[ProducerError, RecordMetadata]
          _ <- promise.complete(beforeComplete(RecordMetadata(topic, partition, offset))).fork
        } yield promise.await

      case ProducerConfig.Failing =>
        ProducerError(new IllegalStateException("Oops")).flip.flatMap(error =>
          Promise.make[ProducerError, RecordMetadata]
            .tap(_.fail(error)).map(_.await))
    }
}

object FakeProducer {
  def make: UIO[FakeProducer] = make()
  def make(beforeProduce: ProducerRecord[Chunk[Byte], Chunk[Byte]] => IO[ProducerError, ProducerRecord[Chunk[Byte], Chunk[Byte]]] = UIO(_),
           beforeComplete: RecordMetadata => IO[ProducerError, RecordMetadata] = UIO(_),
           attributes: Map[String, String] = Map.empty
          ): UIO[FakeProducer] = for {
    records <- Queue.unbounded[ProducerRecord[Chunk[Byte], Chunk[Byte]]]
    offset <- Ref.make(Map.empty[TopicPartition, Offset])
  } yield FakeProducer(records, offset, ProducerConfig.Standard, beforeProduce, beforeComplete, attributes)
}

sealed trait ProducerConfig

object ProducerConfig {

  case object Standard extends ProducerConfig

  case object Failing extends ProducerConfig

}
