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

  def make(specs: ConsumerSpec*): ZManaged[GreyhoundMetrics, Nothing, (OffsetsMap, Handler)] = make {
    specs.foldLeft(Map.empty[TopicName, List[ConsumerSpec]]) { (acc, spec) =>
      spec.topics.foldLeft(acc) { (acc1, topic) =>
        acc1 + (topic -> (spec :: acc1.getOrElse(topic, Nil)))
      }
    }
  }

  def make(specs: Map[TopicName, Seq[ConsumerSpec]]): ZManaged[GreyhoundMetrics, Nothing, (OffsetsMap, Handler)] = for {
    offsets <- Ref.make(Map.empty[TopicPartition, Offset]).toManaged_
    handlers <- createHandlers(specs, updateOffsets(offsets))
    handler = RecordHandler { record: Record[Key, Value] =>
      handlers(record.topic).handle(record)
    }
  } yield (offsets, handler)

  private def createHandlers(specs: Map[TopicName, Seq[ConsumerSpec]],
                             updateOffsets: Handler): ZManaged[GreyhoundMetrics, Nothing, Map[TopicName, Handler]] =
    ZManaged.foreach(specs) {
      case (topic, topicSpecs) =>
        ZManaged.foreach(topicSpecs)(ParallelizeByPartition.make).map { handlers =>
          val handler = handlers.reduce(_ par _) *> updateOffsets
          topic -> handler
        }
    }.map(_.toMap)

  private def updateOffsets(offsets: OffsetsMap): Handler = RecordHandler { record =>
    val topicPartition = new TopicPartition(record.topic, record.partition)
    offsets.update { map =>
      val offset = map.get(topicPartition) match {
        case Some(existing) => record.offset max existing
        case None => record.offset
      }
      map + (topicPartition -> offset)
    }
  }

}

object ParallelizeByPartition {
  type Handler[K, V] = RecordHandler[GreyhoundMetrics, Nothing, K, V]

  private val capacity = 128 // TODO should this be configurable?

  def make(spec: ConsumerSpec): ZManaged[GreyhoundMetrics, Nothing, Handler[Key, Value]] =
    make(spec.handler, spec.parallelism)

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
