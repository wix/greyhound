package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Consumer.{Key, Value}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics._
import com.wixpress.dst.greyhound.core.{Offset, Record, TopicName}
import org.apache.kafka.common.TopicPartition
import zio.{Queue, Ref, ZManaged}

// TODO rename?
object ParallelRecordHandler {
  type Handler = RecordHandler[GreyhoundMetrics, Nothing, Key, Value]
  type OffsetsMap = Ref[Map[TopicPartition, Offset]]

  def make(specs: Map[TopicName, Seq[ConsumerSpec]]): ZManaged[GreyhoundMetrics, Nothing, (OffsetsMap, Handler)] = for {
    offsets <- Ref.make(Map.empty[TopicPartition, Offset]).toManaged_
    updateOffsets = RecordHandler[Any, Nothing, Key, Value] { record =>
      val topicPartition = new TopicPartition(record.topic, record.partition)
      // TODO use the larger offsets if key exists
      offsets.update(_ + (topicPartition -> record.offset))
    }
    handlers <- ZManaged.foreach(specs) {
      case (topic, topicSpecs) =>
        ZManaged.foreach(topicSpecs) { spec =>
          val handler = spec.handler *> updateOffsets
          ParallelizeByPartition.make(handler, spec.parallelism)
        }.map { parallelTopicSpecs =>
          topic -> parallelTopicSpecs.reduce(_ par _)
        }
    }
  } yield {
    val handlersByTopic = handlers.toMap
    val handler: Handler = RecordHandler { record =>
      handlersByTopic(record.topic).handle(record)
    }
    (offsets, handler)
  }

}

object ParallelizeByPartition {
  type Handler[K, V] = RecordHandler[GreyhoundMetrics, Nothing, K, V]

  private val capacity = 128 // TODO should this be configurable?

  def make[R, K, V](handler: RecordHandler[R, Nothing, K, V], parallelism: Int): ZManaged[R with GreyhoundMetrics, Nothing, Handler[K, V]] =
    ZManaged.foreach(0 until parallelism)(makeQueue(handler, _)).map { queues =>
      RecordHandler { record =>
        Metrics.report(SubmittingRecord(record)) *>
          queues(record.partition % queues.length).offer(record)
      }
    }

  private def makeQueue[R, K, V](handler: RecordHandler[R, Nothing, K, V], i: Int) = {
    val queue = for {
      _ <- Metrics.report(StartingRecordsProcessor(i))
      queue <- Queue.bounded[Record[K, V]](capacity)
      _ <- queue.take.flatMap { record =>
        Metrics.report(HandlingRecord(record, i)) *>
          handler.handle(record)
      }.forever.fork
    } yield queue

    queue.toManaged { queue =>
      Metrics.report(StoppingRecordsProcessor(i)) *>
        queue.shutdown
    }
  }
}
