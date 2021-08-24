package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.{Topic, TopicConfig}
import com.wixpress.dst.greyhound.core.admin.{AdminClientConfig, TopicPropertiesResult, AdminClient => AdminClientCore}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.ZIO
import zio.blocking.Blocking

import scala.concurrent.Future

trait AdminClient {
  def createTopics(configs: Set[TopicConfig]): Future[Map[String, Option[Throwable]]]

  def createTopic(config: TopicConfig): Future[Unit]

  def numberOfBrokers: Future[Int]

  def propertiesFor(topic: Topic): Future[TopicPropertiesResult]
}

object AdminClient {

  import GreyhoundRuntime.executionContext

  def create(config: AdminClientConfig): AdminClient = GreyhoundRuntime.Live.unsafeRun {
    ZIO.runtime[Blocking with GreyhoundMetrics].map(runtime => new AdminClient {
      override def createTopics(configs: Set[TopicConfig]): Future[Map[String, Option[Throwable]]] =
        runtime.unsafeRunToFuture {
          AdminClientCore.make(config).use { client =>
            client.createTopics(configs)
          }
        }

      override def createTopic(config: TopicConfig): Future[Unit] =
        createTopics(Set(config)).flatMap { errors =>
          errors.get(config.name).map(maybeError =>
            maybeError.map(error => Future.failed(error)).getOrElse(Future.unit)
          ).getOrElse(Future.unit)
        }

      override def numberOfBrokers: Future[Int] =
        runtime.unsafeRunToFuture(
          AdminClientCore.make(config).use(_.numberOfBrokers))

      override def propertiesFor(topic: Topic): Future[TopicPropertiesResult] =
        runtime.unsafeRunToFuture(AdminClientCore.make(config).use { client =>
          client.propertiesFor(Set(topic)).map(_.getOrElse(topic, TopicPropertiesResult.TopicDoesnExist(topic)))
        })
    })
  }
}
