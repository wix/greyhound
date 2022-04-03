package com.wixpress.dst.greyhound.java.testkit

import java.util

import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig => CoreTopicConfig}
import com.wixpress.dst.greyhound.future.{Environment => ZEnvironment, GreyhoundRuntime}
import com.wixpress.dst.greyhound.java.TopicConfig

import scala.collection.JavaConverters._

class DefaultEnvironment extends Environment {

  private val runtime = GreyhoundRuntime.Live

  private val environment = runtime.unsafeRun(ZEnvironment.make)

  override def kafka: ManagedKafka = new ManagedKafka {

    override def bootstrapServers: util.Set[String] = {
      val servers = new util.HashSet[String]()
      servers.addAll(environment.kafka.bootstrapServers.split(",").toSet.asJava)
      servers
    }

    override def createTopic(config: TopicConfig): Unit = runtime.unsafeRun {
      environment.kafka.createTopic(
        CoreTopicConfig(
          name = config.name,
          partitions = config.partitions,
          replicationFactor = config.replicationFactor,
          cleanupPolicy = config.cleanupPolicy.fold(retention => CleanupPolicy.Delete(retention.toMillis), () => CleanupPolicy.Compact),
          extraProperties = config.extraProperties.asScala.toMap
        )
      )
    }

  }

  override def close(): Unit =
    runtime.unsafeRun(environment.shutdown)

}
