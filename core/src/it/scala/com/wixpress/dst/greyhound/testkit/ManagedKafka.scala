package com.wixpress.dst.greyhound.testkit

import java.util.Properties

import com.wixpress.dst.greyhound.core.TopicConfig
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, Metrics}
import com.wixpress.dst.greyhound.testkit.ManagedKafkaMetric._
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.curator.test.TestingServer
import zio.blocking.{Blocking, effectBlocking}
import zio.{RIO, RManaged}

import scala.reflect.io.Directory

trait ManagedKafka {
  def bootstrapServers: Set[String]

  def createTopic(config: TopicConfig): RIO[Blocking, Unit]
}

object ManagedKafka {

  def make(config: ManagedKafkaConfig): RManaged[Blocking with GreyhoundMetrics, ManagedKafka] = for {
    _ <- embeddedZooKeeper(config.zooKeeperPort)
    logDir <- tempDirectory(s"target/kafka/logs/${config.kafkaPort}")
    kafka <- embeddedKafka(KafkaServerConfig(config.kafkaPort, config.zooKeeperPort, 1234, logDir))
  } yield new ManagedKafka {
    private val adminZkClient = kafka.dataPlaneRequestProcessor.adminZkClient

    override val bootstrapServers: Set[String] =
      Set(s"localhost:${config.kafkaPort}")

    override def createTopic(config: TopicConfig): RIO[Blocking, Unit] = effectBlocking {
      adminZkClient.createTopic(config.name, config.partitions, config.replicationFactor, config.properties)
    }
  }

  private def tempDirectory(path: String) = {
    val acquire = Metrics.report(CreatingTempDirectory(path)) *> effectBlocking(Directory(path))
    acquire.toManaged { dir =>
      Metrics.report(DeletingTempDirectory(path)) *>
        effectBlocking(dir.deleteRecursively()).ignore
    }
  }

  private def embeddedZooKeeper(port: Int) = {
    val acquire = Metrics.report(StartingZooKeeper(port)) *> effectBlocking(new TestingServer(port))
    acquire.toManaged { server =>
      Metrics.report(StoppingZooKeeper) *>
        effectBlocking(server.stop()).ignore
    }
  }

  private def embeddedKafka(config: KafkaServerConfig) = {
    val acquire = for {
      _ <- Metrics.report(StartingKafka(config.port))
      server <- effectBlocking(new KafkaServer(config.toKafkaConfig))
      _ <- effectBlocking(server.startup())
    } yield server
    acquire.toManaged { server =>
      Metrics.report(StoppingKafka) *>
        effectBlocking(server.shutdown()).ignore
    }
  }

}

case class ManagedKafkaConfig(kafkaPort: Int, zooKeeperPort: Int)

object ManagedKafkaConfig {
  val Default: ManagedKafkaConfig = ManagedKafkaConfig(6667, 2181)
}

case class KafkaServerConfig(port: Int,
                             zooKeeperPort: Int,
                             brokerId: Int,
                             logDir: Directory) {

  def toKafkaConfig: KafkaConfig = KafkaConfig.fromProps {
    val props = new Properties
    props.setProperty("port", port.toString)
    props.setProperty("log.dir", logDir.path)
    props.setProperty("zookeeper.connect", s"localhost:$zooKeeperPort")
    props.setProperty("num.partitions", "8")
    props.setProperty("offsets.topic.num.partitions", "5")
    props.setProperty("zookeeper.session.timeout.ms", "15000")
    props.setProperty("host.name", "localhost")
    props.setProperty("listeners", s"PLAINTEXT://:$port")
    props.setProperty("authorizer.class.name", "kafka.security.auth.SimpleAclAuthorizer")
    props.setProperty("super.users", "User:testadmin;")
    props.setProperty("allow.everyone.if.no.acl.found", "true")
    props.setProperty("offsets.topic.replication.factor", "1")
    props.setProperty("group.initial.rebalance.delay.ms", "0")
    props.setProperty("log.cleaner.min.compaction.lag.ms", "3600000")
    props.setProperty("transaction.state.log.replication.factor", "1")
    props.setProperty("transaction.state.log.min.isr", "1")
    props.setProperty("transaction.timeout.ms", "500")
    props.setProperty("transaction.max.timeout.ms", "15000")
    props.setProperty("reserved.broker.max.id", "99999999")
    props.setProperty("auto.create.topics.enable", "false")
    props
  }

}

sealed trait ManagedKafkaMetric extends GreyhoundMetric

object ManagedKafkaMetric {
  case class CreatingTempDirectory(path: String) extends ManagedKafkaMetric
  case class DeletingTempDirectory(path: String) extends ManagedKafkaMetric
  case class StartingZooKeeper(port: Int) extends ManagedKafkaMetric
  case object StoppingZooKeeper extends ManagedKafkaMetric
  case class StartingKafka(port: Int) extends ManagedKafkaMetric
  case object StoppingKafka extends ManagedKafkaMetric
}
