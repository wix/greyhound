package com.wixpress.dst.greyhound.core.admin

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.MetricResult
import zio.Cause.Fail
import zio.Exit

trait AdminClientMetric extends GreyhoundMetric

object AdminClientMetric {
  case class TopicConfigUpdated(topic: Topic,
                                configProperties: Map[String, ConfigPropOp],
                                incremental: Boolean,
                                attributes: Map[String, String],
                                result: MetricResult[Throwable, Unit]) extends AdminClientMetric

  case class TopicPartitionsIncreased(topic: Topic,
                                      newCount: Int,
                                      attributes: Map[String, String],
                                      result: MetricResult[Throwable, Unit])
    extends AdminClientMetric

  case class TopicCreated(topic: Topic, attributes: Map[String, String], result: MetricResult[Throwable, TopicCreateResult]) extends AdminClientMetric
  sealed trait TopicCreateResult
  object TopicCreateResult {
    case object AlreadyExists extends TopicCreateResult
    case object Created extends TopicCreateResult

    def fromExit(topicExists: Throwable => Boolean)(exit: Exit[Throwable, Unit]): Exit[Throwable, TopicCreateResult] = {
      exit match {
        case Exit.Success(_) => Exit.Success(Created)
        case Exit.Failure(cause) if cause.failures.exists(topicExists) => Exit.Success(AlreadyExists)
        case e @ Exit.Failure(_) => e
      }
    }
  }
}
