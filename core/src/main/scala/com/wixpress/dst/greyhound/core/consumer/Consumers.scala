package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{GroupName, TopicName}
import zio.ZManaged
import zio.blocking.Blocking

object Consumers {
  private val clientId = "greyhound-consumers"

  def make(bootstrapServers: Set[String], specs: ConsumerSpec*): ZManaged[Blocking with GreyhoundMetrics, Throwable, Map[GroupName, EventLoop]] =
    ZManaged.foreachPar(specs.groupBy(_.group)) {
      case (group, groupSpecs) =>
        for {
          consumer <- Consumer.make(ConsumerConfig(bootstrapServers, group, clientId))
          handler <- ParallelRecordHandler.make(groupSpecs: _*)
          topics = groupSpecs.foldLeft(Set.empty[TopicName])(_ ++ _.topics)
          eventLoop <- EventLoop.make(consumer, handler._1, handler._2, topics)
        } yield group -> eventLoop
    }.map(_.toMap)
}
