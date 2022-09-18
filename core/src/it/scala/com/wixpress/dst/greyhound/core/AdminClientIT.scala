package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.CleanupPolicy.Delete
import com.wixpress.dst.greyhound.core.admin.AdminClientMetric.TopicConfigUpdated
import com.wixpress.dst.greyhound.core.admin.{ConfigPropOp, GroupState, PartitionOffset, TopicPropertiesResult}
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.{RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, TestMetrics}
import com.wixpress.dst.greyhound.core.zioutils.CountDownLatch
import com.wixpress.dst.greyhound.testenv.ITEnv
import com.wixpress.dst.greyhound.testenv.ITEnv.{testResources, Env, TestResources}
import org.apache.kafka.common.config.TopicConfig.{DELETE_RETENTION_MS_CONFIG, MAX_MESSAGE_BYTES_CONFIG, RETENTION_MS_CONFIG}
import org.apache.kafka.common.errors.InvalidTopicException
import org.specs2.specification.core.Fragments
import zio.Duration.fromScala
import zio.{Chunk, Ref, ZIO}

import scala.concurrent.duration._

class AdminClientIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env = ITEnv.ManagedEnv

  override def sharedEnv = ITEnv.testResources()

  val resources = testResources()

  "createTopics" should {
    "create topics" in {

      val topic1, topic2 = aTopicConfig()
      for {
        testResources <- getShared
        created       <- testResources.kafka.adminClient.createTopics(Set(topic1, topic2))
        topicsAfter   <- testResources.kafka.adminClient.listTopics()
      } yield {
        (created === Map(topic1.name -> None, topic2.name -> None)) and (topicsAfter.toSeq must contain(topic1.name, topic2.name))
      }
    }

    "topic exist" in {
      val topic1 = aTopicConfig()

      for {
        r      <- getShared
        before <- r.kafka.adminClient.topicExists(topic1.name)
        _      <- r.kafka.adminClient.createTopics(Set(topic1))
        after  <- r.kafka.adminClient.topicExists(topic1.name)
      } yield {
        after === true and before === false
      }
    }

    "topics exist" in {
      val topic1 = aTopicConfig()
      val topic2 = aTopicConfig()
      val topic3 = aTopicConfig()
      val topics = Set[String](topic1.name, topic2.name, topic3.name)

      for {
        r            <- getShared
        before       <- r.kafka.adminClient.topicExists(topic1.name)
        _            <- r.kafka.adminClient.createTopics(Set(topic1))
        _            <- r.kafka.adminClient.createTopics(Set(topic2))
        topicsExists <- r.kafka.adminClient.topicsExist(topics)
      } yield {
        (topicsExists(topic1.name) must beTrue and (before must beFalse)) and
          (topicsExists(topic2.name) must beTrue) and
          (topicsExists(topic3.name) must beFalse)
      }
    }

    "topic does not exist" in {
      for {
        r      <- getShared
        result <- r.kafka.adminClient.topicExists("missing topic")
      } yield {
        result === false
      }
    }

    "reflect errors" in {
      val topic1 = aTopicConfig()
      val topic2 = aTopicConfig("x" * 250)
      for {
        r       <- getShared
        created <- r.kafka.adminClient.createTopics(Set(topic1, topic2))
      } yield {
        (created(topic1.name) must beNone) and (created(topic2.name) must beSome(beAnInstanceOf[InvalidTopicException]))
      }
    }

    "ignore errors based on filter" in {
      val badTopic = aTopicConfig("x" * 250)
      for {
        r       <- getShared
        created <- r.kafka.adminClient.createTopics(Set(badTopic), ignoreErrors = _.isInstanceOf[InvalidTopicException])
      } yield {
        created === Map(badTopic.name -> None)
      }
    }

    "ignore TopicExistsException by default" in {
      val topic = aTopicConfig()
      for {
        r        <- getShared
        created1 <- r.kafka.adminClient.createTopics(Set(topic))
        created2 <- r.kafka.adminClient.createTopics(Set(topic))
      } yield {
        (created1 === Map(topic.name -> None)) and (created2 === Map(topic.name -> None))
      }
    }

    "list groups" in {
      val topic = aTopicConfig()
      for {
        r      <- getShared
        group   = "group1"
        groups <- ZIO.scoped(
                    RecordConsumer
                      .make(RecordConsumerConfig(r.kafka.bootstrapServers, group, Topics(Set(topic.name))), RecordHandler.empty)
                      .flatMap { _ => r.kafka.adminClient.listGroups() }
                  )
      } yield {
        groups === Set(group)
      }
    }

    "fetch group offsets" in {
      val topic = aTopicConfig()
      ZIO.scoped(for {
        r                                 <- getShared
        _                                 <- r.kafka.adminClient.createTopics(Set(topic))
        groupOffsetsRef                   <- Ref.make[Map[GroupTopicPartition, PartitionOffset]](Map.empty)
        calledGroupsTopicsAfterAssignment <- CountDownLatch.make(1)
        group                              = s"group1-${System.currentTimeMillis}"
        handler                            = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                                               {
                                                 r.kafka.adminClient.groupOffsets(Set(group)).flatMap(r => groupOffsetsRef.set(r)) *>
                                                   calledGroupsTopicsAfterAssignment.countDown
                                               }
                                             }
        _                                 <- RecordConsumer
                                               .make(RecordConsumerConfig(r.kafka.bootstrapServers, group, Topics(Set(topic.name))), handler)
        awaitResultAndGroupOffsets        <- for {
                                               recordPartition <- ZIO.succeed(ProducerRecord(topic.name, Chunk.empty, partition = Some(0)))
                                               _               <- r.producer.produce(recordPartition)
                                               awaitResult     <- calledGroupsTopicsAfterAssignment.await.timeout(fromScala(20.seconds))
                                               groupOffsets    <- groupOffsetsRef.get
                                             } yield (awaitResult, groupOffsets)
      } yield {
        (awaitResultAndGroupOffsets._1 aka "awaitResult" must not(beNone)) and
          (awaitResultAndGroupOffsets._2 === Map(GroupTopicPartition(group, TopicPartition(topic.name, 0)) -> PartitionOffset(0L)))
      })
    }

    "fetch group state when consumer started and when consumer is shut down" in {
      val partitionsCount = 2
      val topic           = aTopicConfig(partitions = partitionsCount)
      for {
        r                                <- getShared
        _                                <- r.kafka.adminClient.createTopics(Set(topic))
        groupStateRef                    <- Ref.make[Option[GroupState]](None)
        calledGroupsStateAfterAssignment <- CountDownLatch.make(1)
        group                             = "group1"
        handler                           = RecordHandler { _: ConsumerRecord[Chunk[Byte], Chunk[Byte]] =>
                                              {
                                                ZIO.debug("in record handler....") *>
                                                  r.kafka.adminClient.groupState(Set(group)).flatMap(r => groupStateRef.set(r.get(group))) *> ZIO.debug("yay!....") *>
                                                  calledGroupsStateAfterAssignment.countDown
                                              }
                                            }
        awaitResultAndStateWhenStarted   <-
          ZIO.scoped(
            RecordConsumer
              .make(RecordConsumerConfig(r.kafka.bootstrapServers, group, Topics(Set(topic.name))), handler)
              .flatMap { _ =>
                for {
                  recordPartition  <- ZIO.succeed(ProducerRecord(topic.name, Chunk.empty, partition = Some(0)))
                  _                <- r.producer
                                        .produce(recordPartition)
                                        .tap(rm => ZIO.debug(s"produced!! offset ${rm.offset}"))
                  awaitResult      <- calledGroupsStateAfterAssignment.await.timeout(fromScala(10.seconds))
                  stateWhenStarted <- groupStateRef.get
                } yield (awaitResult, stateWhenStarted)
              }
          )

        stateAfterShutdown <- r.kafka.adminClient.groupState(Set(group)).map(_.get(group))
      } yield {
        (awaitResultAndStateWhenStarted._1 aka "awaitResult" must not(beNone)) and
          (awaitResultAndStateWhenStarted._2 === Some(GroupState(Set(TopicPartition(topic.name, 0), TopicPartition(topic.name, 1))))) and
          (stateAfterShutdown === Some(GroupState(Set.empty)))
      }
    }

    "propertiesFor" in {
      val topics           = (1 to 3).map(i => aTopicConfig(partitions = i, props = Map(RETENTION_MS_CONFIG -> (i * 100000).toString)))
      val nonExistentTopic = "non-existent-topic"
      for {
        r          <- getShared
        _          <- r.kafka.adminClient.createTopics(topics.toSet)
        topicProps <- r.kafka.adminClient.propertiesFor(topics.map(_.name).toSet + nonExistentTopic)
      } yield {
        topics.foldLeft(ok) {
          case (acc, tc) =>
            acc and {
              topicProps.get(tc.name) aka s"properties for ${tc.name}" must
                beSome(beLike[TopicPropertiesResult] {
                  case tpr: TopicPropertiesResult.TopicProperties =>
                    (tpr.partitions aka s"${tc.name} partitions" must_=== tc.partitions) and
                      (tpr.properties aka s"${tc.name} properties" must containAllOf(tc.propertiesMap.toSeq))
                })
            }
        } and (topicProps.get(nonExistentTopic) must beSome(TopicPropertiesResult.TopicDoesnExist(nonExistentTopic)))

      }
    }

    "increase partitions" in {
      val topic = aTopicConfig()
      for {
        r     <- getShared
        _     <- r.kafka.adminClient.createTopics(Set(topic))
        _     <- r.kafka.adminClient.increasePartitions(topic.name, topic.partitions + 1)
        props <- r.kafka.adminClient.propertiesFor(Set(topic.name)).flatMap(_.values.head.getOrFail)
      } yield props.partitions === 2
    }

    Fragments.foreach(Seq(false, true)) { nonIncremental =>
      s"set topic properties [nonIncremental: $nonIncremental]" in {
        val topic = aTopicConfig(props = Map(MAX_MESSAGE_BYTES_CONFIG -> "1000000", DELETE_RETENTION_MS_CONFIG -> "123456841"))
        for {
          r             <- getShared
          _             <- r.kafka.adminClient.createTopics(Set(topic))
          _             <- r.kafka.adminClient.updateTopicConfigProperties(
                             topic.name,
                             Map(
                               MAX_MESSAGE_BYTES_CONFIG   -> ConfigPropOp.Set("2000000"),
                               RETENTION_MS_CONFIG        -> ConfigPropOp.Set("3000000"),
                               DELETE_RETENTION_MS_CONFIG -> ConfigPropOp.Delete
                             ),
                             nonIncremental
                           )
          props         <- r.kafka.adminClient.propertiesFor(Set(topic.name)).flatMap(_.values.head.getOrFail)
          updatedEvents <- TestMetrics.reportedOf[TopicConfigUpdated]()
        } yield (props.properties.toSeq must
          contain(
            MAX_MESSAGE_BYTES_CONFIG -> "2000000",
            RETENTION_MS_CONFIG      -> "3000000"
          )) &&
          // the deleted property will still have value, but this will be broker level default
          (props.propertiesThat(_.isTopicSpecific).get(DELETE_RETENTION_MS_CONFIG) must beNone) &&
          (updatedEvents must contain(like[TopicConfigUpdated] { case tcu => tcu.incremental === !nonIncremental }))
      }
    }
  }

  private def aTopicConfig(name: String = s"topic-${System.nanoTime}", partitions: Int = 1, props: Map[String, String] = Map.empty) =
    TopicConfig(name, partitions, 1, Delete(1.hour.toMillis), props)
}
