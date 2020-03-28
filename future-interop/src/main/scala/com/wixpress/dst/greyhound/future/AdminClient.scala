package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.TopicConfig
import com.wixpress.dst.greyhound.core.admin.{AdminClientConfig, AdminClient => AdminClientCore}
import zio.ZIO
import zio.blocking.Blocking

import scala.concurrent.Future

trait AdminClient {
  def createTopics(configs: Set[TopicConfig]): Future[Map[String, Option[Throwable]]]

  def createTopic(config: TopicConfig): Future[Unit]

  def numberOfBrokers: Future[Int]
}

object AdminClient {

  import GreyhoundRuntime.executionContext

  def create(config: AdminClientConfig): AdminClient = GreyhoundRuntime.Live.unsafeRun {
    ZIO.runtime[Blocking].map(runtime => new AdminClient {
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
    })
  }
}