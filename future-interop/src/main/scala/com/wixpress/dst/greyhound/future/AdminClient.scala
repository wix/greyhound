package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.{Topic, TopicConfig}
import com.wixpress.dst.greyhound.core.admin.{AdminClient => AdminClientCore, AdminClientConfig, TopicPropertiesResult}
import zio.ZIO


import scala.concurrent.Future

trait AdminClient {
  def createTopics(configs: Set[TopicConfig]): Future[Map[String, Option[Throwable]]]

  def createTopic(config: TopicConfig): Future[Unit]

  def numberOfBrokers: Future[Int]

  def propertiesFor(topic: Topic): Future[TopicPropertiesResult]
}

object AdminClient {

  import GreyhoundRuntime.executionContext

  def create(config: AdminClientConfig): AdminClient =
    new AdminClient {
      override def createTopics(configs: Set[TopicConfig]): Future[Map[String, Option[Throwable]]] =
        GreyhoundRuntime.Live.unsafeRunToFuture {
          ZIO.scoped(AdminClientCore.make(config).flatMap { client => client.createTopics(configs) })
        }

      override def createTopic(config: TopicConfig): Future[Unit] =
        createTopics(Set(config)).flatMap { errors =>
          errors
            .get(config.name)
            .map(maybeError => maybeError.map(error => Future.failed(error)).getOrElse(Future.unit))
            .getOrElse(Future.unit)
        }

      override def numberOfBrokers: Future[Int] =
        GreyhoundRuntime.Live.unsafeRunToFuture(ZIO.scoped(AdminClientCore.make(config).flatMap(_.numberOfBrokers)))

      override def propertiesFor(topic: Topic): Future[TopicPropertiesResult] =
        GreyhoundRuntime.Live.unsafeRunToFuture(ZIO.scoped(AdminClientCore.make(config).flatMap { client =>
          client.propertiesFor(Set(topic)).map(_.getOrElse(topic, TopicPropertiesResult.TopicDoesnExist(topic)))
        }))
    }
}
