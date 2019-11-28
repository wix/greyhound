package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Record
import com.wixpress.dst.greyhound.core.consumer.Consumer.{Key, Value}
import zio.blocking.Blocking
import zio.console.Console
import zio.duration._
import zio.{ZIO, ZManaged}

import scala.collection.JavaConverters._

object Consumers {
  private val clientId = "greyhound-consumers"

  def start(bootstrapServers: Set[String], specs: ConsumerSpec*): ZIO[Blocking with Console, Throwable, Nothing] =
    ZIO.foreachPar(specs.groupBy(_.group)) {
      case (group, groupSpecs) =>
        val topicSpecs = groupSpecs.groupBy(_.topic)
        val makeConsumer = Consumer.make(ConsumerConfig(bootstrapServers, group, clientId))
        (makeConsumer zip makeHandler(topicSpecs)).use {
          case (consumer, handler) =>
            consumer.subscribe(topicSpecs.keySet) *>
              consumer.poll(100.millis).flatMap { records =>
                ZIO.foreach_(records.asScala) { record =>
                  handler.handle(Record(record))
                }
              }.forever
        }
    } *> ZIO.never

  // TODO parallelize
  // TODO change to Map[String, ConsumerSpec] (single consumer spec per group and topic)
  private def makeHandler(specs: Map[String, Seq[ConsumerSpec]]) =
    ZManaged.succeed {
      RecordHandler[Any, Nothing, Key, Value] { record =>
        specs(record.topic).head.handler.handle(record)
      }
    }

}
