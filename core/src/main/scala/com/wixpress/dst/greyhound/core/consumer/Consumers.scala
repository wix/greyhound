package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Record
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{Metrics, Subscribing}
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
        val topicSpecs = groupSpecs.groupBy(_.topic)
        val makeConsumer = Consumer.make(ConsumerConfig(bootstrapServers, group, clientId))
        (makeConsumer zip ParallelRecordHandler.make(topicSpecs)).use {
          case (consumer, (offsets, handler)) =>
            val topics = topicSpecs.keySet
            Metrics.report(Subscribing(topics)) *>
              consumer.subscribe(topics) *>
              consumer.poll(pollTimeout).flatMap { records =>
                ZIO.foreach_(records.asScala) { record =>
                  handler.handle(Record(record))
                }
              }.forever
        }
    } *> ZIO.never
}
