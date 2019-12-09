package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{CommittingOffsets, Metrics, Subscribing}
import com.wixpress.dst.greyhound.core.{Record, TopicName}
import zio.ZIO
import zio.blocking.Blocking
import zio.duration._

import scala.collection.JavaConverters._

object Consumers {
  private val clientId = "greyhound-consumers"
  private val pollTimeout = 100.millis

  def start(bootstrapServers: Set[String], specs: ConsumerSpec*): ZIO[GreyhoundMetrics with Blocking, Throwable, Nothing] =
    ZIO.foreachPar(specs.groupBy(_.group)) {
      case (group, groupSpecs) =>
        val topics = groupSpecs.flatMap(_.topics).toSet
        val makeConsumer = Consumer.make(ConsumerConfig(bootstrapServers, group, clientId))
        (makeConsumer zip ParallelRecordHandler.make(groupSpecs: _*)).use {
          case (consumer, (offsets, handler)) =>
            subscribe(consumer, topics) *>
              (pollAndHandle(consumer, handler) *>
                commitOffsets(consumer, offsets)).forever
        }
    } *> ZIO.never

  private def subscribe(consumer: Consumer, topics: Set[TopicName]) =
    Metrics.report(Subscribing(topics)) *> consumer.subscribe(topics)

  private def pollAndHandle(consumer: Consumer, handler: ParallelRecordHandler.Handler) =
    consumer.poll(pollTimeout).flatMap { records =>
      ZIO.foreach_(records.asScala) { record =>
        handler.handle(Record(record))
      }
    }

  private def commitOffsets(consumer: Consumer, offsets: ParallelRecordHandler.OffsetsMap) =
    offsets.modify(current => (current, Map.empty)).flatMap { current =>
      ZIO.when(current.nonEmpty) {
        Metrics.report(CommittingOffsets(current)) *>
          consumer.commit(current)
      }
    }
}
