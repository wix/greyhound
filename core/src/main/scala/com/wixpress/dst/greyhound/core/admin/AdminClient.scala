package com.wixpress.dst.greyhound.core.admin

import java.util.Properties

import com.wixpress.dst.greyhound.core.TopicConfig
import org.apache.kafka.clients.admin.{NewTopic, AdminClient => KafkaAdminClient, AdminClientConfig => KafkaAdminClientConfig}
import zio.blocking.{Blocking, effectBlocking}
import zio.{RIO, RManaged, ZIO, ZManaged}

import scala.collection.JavaConverters._

trait AdminClient {
  def createTopics(configs: Set[TopicConfig]): RIO[Blocking, Map[String, Option[Throwable]]]
}

object AdminClient {
  def make(config: AdminClientConfig): RManaged[Blocking, AdminClient] = {
    val acquire = effectBlocking(KafkaAdminClient.create(config.properties))
    ZManaged.make(acquire)(client => effectBlocking(client.close()).ignore).map { client =>
      new AdminClient {
        override def createTopics(configs: Set[TopicConfig]): RIO[Blocking, Map[String, Option[Throwable]]] =
          effectBlocking(client.createTopics(configs.map(toNewTopic).asJava)).flatMap { result =>
            ZIO.foreach(result.values.asScala) {
              case (topic, topicResult) =>
                ZIO.effectAsync[Any, Nothing, (String, Option[Throwable])] { cb =>
                  topicResult.whenComplete { (_: Void, exception: Throwable) =>
                    cb(ZIO.succeed(topic -> Option(exception)))
                  }
                }
            }.map(_.toMap)
          }

        private def toNewTopic(config: TopicConfig): NewTopic =
          new NewTopic(config.name, config.partitions, config.replicationFactor.toShort)
            .configs(config.propertiesMap.asJava)
      }
    }
  }
}

case class AdminClientConfig(bootstrapServers: Set[String],
                             extraProperties: Map[String, String] = Map.empty) {

  def properties: Properties = {
    val props = new Properties
    props.setProperty(KafkaAdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.mkString(","))
    extraProperties.foreach {
      case (key, value) =>
        props.setProperty(key, value)
    }
    props
  }

}
