package com.wixpress.dst.greyhound.core.admin

import java.util.concurrent.TimeUnit.SECONDS
import com.wixpress.dst.greyhound.core
import com.wixpress.dst.greyhound.core.admin.AdminClient.isTopicExistsError
import com.wixpress.dst.greyhound.core.admin.TopicPropertiesResult.{TopicDoesnExistException, TopicProperties}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.zioutils.KafkaFutures._
import com.wixpress.dst.greyhound.core.{CommonGreyhoundConfig, Group, GroupTopicPartition, Topic, TopicConfig, TopicPartition}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
import org.apache.kafka.clients.admin.{AlterConfigOp, NewPartitions, NewTopic, AdminClient => KafkaAdminClient, AdminClientConfig => KafkaAdminClientConfig}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.clients.admin.{Config, ConfigEntry}
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC
import org.apache.kafka.common.errors.{TopicExistsException, UnknownTopicOrPartitionException}
import zio._
import zio.blocking.{Blocking, effectBlocking}
import GreyhoundMetrics._
import com.wixpress.dst.greyhound.core.admin.AdminClientMetric.TopicCreateResult.fromExit
import com.wixpress.dst.greyhound.core.admin.AdminClientMetric.{TopicConfigUpdated, TopicCreateResult, TopicCreated, TopicPartitionsIncreased}

import scala.collection.JavaConverters._

trait AdminClient {
  def listTopics(): RIO[Blocking, Set[String]]

  def topicExists(topic: String): RIO[Blocking, Boolean]

  def createTopics(configs: Set[TopicConfig], ignoreErrors: Throwable => Boolean = isTopicExistsError): RIO[Blocking with GreyhoundMetrics, Map[String, Option[Throwable]]]

  def numberOfBrokers: RIO[Blocking, Int]

  def propertiesFor(topics: Set[Topic]): RIO[Blocking, Map[Topic, TopicPropertiesResult]]

  def listGroups(): RIO[Blocking, Set[String]]

  def groupOffsets(groups: Set[String]): RIO[Blocking, Map[GroupTopicPartition, PartitionOffset]]

  def groupState(groups: Set[String]): RIO[Blocking, Map[String, GroupState]]

  def deleteTopic(topic: Topic): RIO[Blocking, Unit]

  def describeConsumerGroups(groupIds: Set[Group]): RIO[Blocking, Map[Group, ConsumerGroupDescription]]

  def increasePartitions(topic: Topic, newCount: Int): RIO[Blocking with GreyhoundMetrics, Unit]

  /**
   * @param useNonIncrementalAlter - [[org.apache.kafka.clients.admin.AdminClient.incrementalAlterConfigs()]] is not supported by older brokers (< 2.3),
   *                               so if this is true, use the deprecated non incremental alter
   */
  def updateTopicConfigProperties(topic: Topic,
                                  configProperties: Map[String, ConfigPropOp],
                                  useNonIncrementalAlter: Boolean = false): RIO[Blocking with GreyhoundMetrics, Unit]

  def attributes: Map[String, String]

}

sealed trait ConfigPropOp
object ConfigPropOp {
  case object Delete extends ConfigPropOp
  case class Set(value: String) extends ConfigPropOp
}

sealed trait TopicPropertiesResult {
  def topic: Topic
  def toOption: Option[TopicPropertiesResult.TopicProperties]
  def getOrThrow: TopicPropertiesResult.TopicProperties = toOption.getOrElse(throw new TopicDoesnExistException(topic))
  def getOrFail: IO[TopicDoesnExistException, TopicProperties] =
    ZIO.fromOption(toOption).orElseFail(throw new TopicDoesnExistException(topic))
}
object TopicPropertiesResult {
  case class TopicProperties(topic: Topic, partitions: Int, configEntries: Seq[TopicConfigEntry], replications: Int) extends TopicPropertiesResult {
    val properties = propertiesThat((_:TopicConfigEntry) => true)
    def propertiesThat(filter: TopicConfigEntry => Boolean) = configEntries.filter(filter).map(e => e.key -> e.value).toMap
    override def toOption: Option[TopicPropertiesResult.TopicProperties] = Some(this)
  }
  case class TopicDoesnExist(topic: Topic) extends TopicPropertiesResult {
    override def toOption: Option[TopicPropertiesResult.TopicProperties] = None
  }
  def apply(topic: Topic, partitions: Int, configEntries: Seq[TopicConfigEntry], replications: Int): TopicProperties =
    TopicProperties(topic, partitions, configEntries, replications)

  class TopicDoesnExistException(topic: String) extends
    RuntimeException(s"Failed to fetch properties for non existent topic: $topic")
}

case class TopicConfigEntry(key: String, value: String, source: ConfigSource) {
  def isTopicSpecific: Boolean = source == ConfigSource.DYNAMIC_TOPIC_CONFIG
}

case class PartitionOffset(offset: Long)

case class GroupState(activeTopicsPartitions: Set[TopicPartition])

object AdminClient {
  def make(config: AdminClientConfig,
           clientAttributes: Map[String, String] = Map.empty): RManaged[Blocking, AdminClient] = {
    val acquire = effectBlocking(KafkaAdminClient.create(config.properties))
    ZManaged.make(acquire)(client => effectBlocking(client.close()).ignore).map { client =>
      new AdminClient {
        override def topicExists(topic: String): RIO[Blocking, Boolean] =
          effectBlocking(client.describeTopics(Seq(topic).asJava)).flatMap { result =>
            result.values().asScala.headOption.map { case (_, topicResult) => topicResult.asZio.either.flatMap {
              case Right(_) => UIO(true)
              case Left(_: UnknownTopicOrPartitionException) => UIO(false)
              case Left(ex) => ZIO.fail(ex)
            }
            }.getOrElse(UIO(false))
          }

        override def createTopics(configs: Set[TopicConfig], ignoreErrors: Throwable => Boolean = isTopicExistsError): RIO[Blocking with GreyhoundMetrics, Map[String, Option[Throwable]]] =
          effectBlocking(client.createTopics(configs.map(toNewTopic).asJava)).flatMap { result =>
            ZIO.foreach(result.values.asScala.toSeq) {
              case (topic, topicResult) =>
                topicResult.asZio.unit
                  .reporting(res => TopicCreated(topic, attributes, res.mapExit(fromExit(isTopicExistsError))))
                  .either
                  .map(topic -> _.left.toOption.filterNot(ignoreErrors))
            }.map(_.toMap)
          }

        override def numberOfBrokers: RIO[Blocking, Int] =
          effectBlocking(client.describeCluster()).flatMap(result =>
            effectBlocking {
              result.nodes().get(30, SECONDS)
            }.map(x => x.asScala.toSeq.size))


        override def propertiesFor(topics: Set[Topic]): RIO[Blocking, Map[Topic, TopicPropertiesResult]] =
          (describeConfigs(client, topics) zipPar describePartitions(client, topics)).map {
            case (configsPerTopic, partitionsAndReplicationPerTopic) =>
              partitionsAndReplicationPerTopic
                .map(pair => pair -> configsPerTopic.getOrElse(pair._1, TopicPropertiesResult.TopicDoesnExist(pair._1)))
                .map {
                  case ((topic, TopicProperties(_, partitions, _, replication)), TopicProperties(_, _, propertiesMap, _)) =>
                    topic -> TopicPropertiesResult(topic, partitions, propertiesMap, replication)
                  case ((topic, _), _) => topic -> TopicPropertiesResult.TopicDoesnExist(topic)
                }
          }


        override def listTopics(): RIO[Blocking, Set[String]] = for {
          result <- effectBlocking(client.listTopics())
          topics <- result.names().asZio
        } yield topics.asScala.toSet

        private def toNewTopic(config: TopicConfig): NewTopic =
          new NewTopic(config.name, config.partitions, config.replicationFactor.toShort)
            .configs(config.propertiesMap.asJava)

        override def listGroups(): RIO[Blocking, Set[String]] = for {
          result <- effectBlocking(client.listConsumerGroups())
          groups <- result.valid().asZio
        } yield groups.asScala.map(_.groupId()).toSet

        override def groupOffsets(groups: Set[String]): RIO[Blocking, Map[GroupTopicPartition, PartitionOffset]] =
          for {
            result <- ZIO.foreach(groups)(group => effectBlocking(group -> client.listConsumerGroupOffsets(group)))
            // TODO: remove ._1 , ._2
            rawOffsetsEffects = result.toMap.mapValues(_.partitionsToOffsetAndMetadata().asZio)
            offsetsEffects = rawOffsetsEffects.map(offset => offset._2.map(f => f.asScala.map(p => p.copy(GroupTopicPartition(offset._1, core.TopicPartition(p._1)), PartitionOffset(p._2.offset())))))
            offsetsMapSets <- ZIO.collectAll(offsetsEffects)
            groupOffsets = offsetsMapSets.foldLeft(Map.empty[GroupTopicPartition, PartitionOffset])((x, y) => x ++ y)
          } yield groupOffsets

        override def groupState(groups: Set[String]): RIO[Blocking, Map[String, GroupState]] =
          for {
            result <- effectBlocking(client.describeConsumerGroups(groups.asJava))
            groupEffects = result.describedGroups().asScala.mapValues(_.asZio)
            groupsList <- ZIO.collectAll(groupEffects.values)
            membersMap = groupsList.groupBy(_.groupId()).mapValues(_.flatMap(_.members().asScala))
            groupState = membersMap.mapValues(members => {
              val topicPartitionsMap = members.flatMap(_.assignment().topicPartitions().asScala)
              GroupState(topicPartitionsMap.map(TopicPartition(_)).toSet)
            }
            )
          } yield groupState

        override def deleteTopic(topic: Topic): RIO[Blocking, Unit] = {
          effectBlocking(client.deleteTopics(Set(topic).asJava).all())
            .flatMap(_.asZio).unit
        }

        override def describeConsumerGroups(groupIds: Set[Group]): RIO[Blocking, Map[Group, ConsumerGroupDescription]] = {
          for {
            desc <- effectBlocking(client.describeConsumerGroups(groupIds.asJava).all())
            all <- desc.asZio
          } yield all.asScala.toMap.mapValues(ConsumerGroupDescription.apply)
        }

        override def increasePartitions(topic: Topic,
                                        newCount: Int): RIO[Blocking with GreyhoundMetrics, Unit] = {
          effectBlocking(client.createPartitions(Map(topic -> NewPartitions.increaseTo(newCount)).asJava))
            .flatMap(_.all().asZio).unit
            .reporting(TopicPartitionsIncreased(topic, newCount, attributes, _))
        }

        override def updateTopicConfigProperties(topic: Topic,
                                                 configProperties: Map[String, ConfigPropOp],
                                                 useNonIncrementalAlter: Boolean = false
                                                ): RIO[Blocking with GreyhoundMetrics, Unit] = {
          if (useNonIncrementalAlter) updateTopicConfigUsingAlter(topic, configProperties)
          else updateTopicConfigIncremental(topic, configProperties)
        }

        override def attributes: Map[String, String] = clientAttributes

        private def updateTopicConfigUsingAlter(topic: Topic,
                                     configProperties: Map[String, ConfigPropOp]) = {
          val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
          (
            for {
            described <- describeConfigs(client, Set(topic))
            beforeProps <- described.values.head.getOrFail
            beforeConfig = beforeProps.propertiesThat(_.isTopicSpecific)
            configToSet = configProperties.foldLeft(beforeConfig) {
              case (acc, (key, ConfigPropOp.Delete)) => acc - key
              case (acc, (key, ConfigPropOp.Set(value))) => acc + (key -> value)
            }
            configJava = new Config(configToSet.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava)
            _ <- effectBlocking(client.alterConfigs(Map(resource -> configJava).asJava))
              .flatMap(_.all().asZio)
            } yield ()
          ).reporting(TopicConfigUpdated(topic, configProperties, incremental = false, attributes, _))
        }

        private def updateTopicConfigIncremental(topic: Topic,
                                                 configProperties: Map[String, ConfigPropOp]) = {
          val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
          val ops = configProperties.map { case (key, value) =>
            value match {
              case ConfigPropOp.Delete => new AlterConfigOp(new ConfigEntry(key, null), OpType.DELETE)
              case ConfigPropOp.Set(value) => new AlterConfigOp(new ConfigEntry(key, value), OpType.SET)
            }
          }.asJavaCollection
          effectBlocking(client.incrementalAlterConfigs(Map(resource -> ops).asJava))
            .flatMap(_.all().asZio).unit
            .reporting(TopicConfigUpdated(topic, configProperties, incremental = true, attributes, _))
        }
      }
    }
  }



  private def describeConfigs(client: KafkaAdminClient, topics: Set[Topic]): RIO[Blocking, Map[Topic, TopicPropertiesResult]] =
    effectBlocking(client.describeConfigs(topics.map(t => new ConfigResource(TOPIC, t)).asJavaCollection)) flatMap { result =>
      ZIO.collectAll(
        result.values.asScala.toMap.map { case (resource, kf) =>
          kf.asZio
           .map { config =>
             resource.name -> TopicPropertiesResult.TopicProperties(resource.name,
               0,
               config.entries().asScala.map(entry => TopicConfigEntry(entry.name, entry.value, entry.source)).toSeq,
               0)
           }.catchSome {
             case _:  UnknownTopicOrPartitionException => UIO(resource.name -> TopicPropertiesResult.TopicDoesnExist(resource.name))
           }
        }
      ).map(_.toMap)
    }

  private def describePartitions(client: KafkaAdminClient, topics: Set[Topic]): RIO[Blocking, Map[Topic, TopicPropertiesResult]] =
    effectBlocking(client.describeTopics(topics.asJavaCollection))
      .flatMap { result =>
        ZIO.collectAll(result.values.asScala.toMap.map { case (topic, kf) =>
          kf.asZio
            .map { desc =>
              val replication = desc.partitions.asScala.map(_.replicas.size).sorted.headOption.getOrElse(0)
              topic -> TopicPropertiesResult.TopicProperties(
                topic,
                desc.partitions.size,
                Seq.empty,
                replication
              )
            }.catchSome {
              case _:  UnknownTopicOrPartitionException => UIO(topic -> TopicPropertiesResult.TopicDoesnExist(topic))
            }
          }
        ).map(_.toMap)
      }

  def isTopicExistsError(e: Throwable): Boolean = e.isInstanceOf[TopicExistsException] ||
    Option(e.getCause).exists(_.isInstanceOf[TopicExistsException])
}

case class AdminClientConfig(bootstrapServers: String,
                             extraProperties: Map[String, String] = Map.empty) extends CommonGreyhoundConfig {


  override def kafkaProps: Map[String, String] =
    Map(KafkaAdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers) ++ extraProperties
}
