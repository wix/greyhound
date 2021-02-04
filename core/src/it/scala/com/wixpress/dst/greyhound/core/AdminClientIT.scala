package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.CleanupPolicy.Delete
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.Topics
import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.core.consumer.{RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.{BaseTestWithSharedEnv, CountDownLatch}
import com.wixpress.dst.greyhound.testkit.ITEnv
import com.wixpress.dst.greyhound.testkit.ITEnv.{Env, TestResources, testResources}
import org.apache.kafka.common.errors.InvalidTopicException
import zio.duration.Duration.fromScala
import zio.{Chunk, Ref, UIO, UManaged}

import scala.concurrent.duration._

class AdminClientIT extends BaseTestWithSharedEnv[Env, TestResources] {
  sequential

  override def env: UManaged[ITEnv.Env] = ITEnv.ManagedEnv

  override def sharedEnv = ITEnv.testResources()

  val resources = testResources()

  "createTopics" should {
    "create topics" in {
      val topic1, topic2 = aTopicConfig()
      for {
        TestResources(kafka, _) <- getShared
        created <- kafka.adminClient.createTopics(Set(topic1, topic2))
        topicsAfter <- kafka.adminClient.listTopics()
      } yield {
        (created === Map(topic1.name -> None, topic2.name -> None)) and
          (topicsAfter.toSeq must contain(topic1.name, topic2.name))
      }
    }

    "topic exist" in {
      val topic1 = aTopicConfig()

      for {
        TestResources(kafka, _) <- getShared
        _ <- kafka.adminClient.createTopics(Set(topic1))
        result <- kafka.adminClient.topicExists(topic1.name)
      } yield {
        result === true
      }
    }

    "topic does not exist" in {
      for {
        TestResources(kafka, _) <- getShared
        result <- kafka.adminClient.topicExists("missing topic")
      } yield {
        result === false
      }
    }

    "reflect errors" in {
      val topic1 = aTopicConfig()
      val topic2 = aTopicConfig("x" * 250)
      for {
        TestResources(kafka, _) <- getShared
        created <- kafka.adminClient.createTopics(Set(topic1, topic2))
      } yield {
        (created(topic1.name) must beNone)  and
        (created(topic2.name) must beSome(beAnInstanceOf[InvalidTopicException]))
      }
    }

    "ignore errors based on filter" in {
      val badTopic = aTopicConfig("x" * 250)
      for {
        TestResources(kafka, _) <- getShared
        created <- kafka.adminClient.createTopics(Set(badTopic), ignoreErrors = _.isInstanceOf[InvalidTopicException])
      } yield {
        created === Map(badTopic.name -> None)
      }
    }

    "ignore TopicExistsException by default" in {
      val topic = aTopicConfig()
      for {
        TestResources(kafka, _) <- getShared
        created1 <- kafka.adminClient.createTopics(Set(topic))
        created2 <- kafka.adminClient.createTopics(Set(topic))
      } yield {
        (created1 === Map(topic.name -> None)) and
          (created2 === Map(topic.name -> None))
      }
    }

    "list groups" in {
      val topic = aTopicConfig()
      for {
        TestResources(kafka, _) <- getShared
        group = "group1"
        groups <- RecordConsumer.make( RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic.name))),
          RecordHandler.empty).use{ _ =>
          kafka.adminClient.listGroups()
        }
      } yield {
        (groups === Set(group))
      }
    }

    "fetch group topics after partitions assigned" in {
      val topic = aTopicConfig()
      for {
        TestResources(kafka, producer) <- getShared
        _ <- kafka.adminClient.createTopics(Set(topic))
        groupTopicsRef <- Ref.make[Map[String, Set[Topic]]](Map.empty)
        calledGroupsTopicsAfterAssignment <- CountDownLatch.make(1)
        group = "group1"
        handler = RecordHandler{_: ConsumerRecord[Chunk[Byte], Chunk[Byte]] => {
          kafka.adminClient.groupTopics(Set(group)).flatMap(r => groupTopicsRef.set(r)) *>
            calledGroupsTopicsAfterAssignment.countDown
        }}
        (awaitResult, groupTopics) <- RecordConsumer.make( RecordConsumerConfig(kafka.bootstrapServers, group, Topics(Set(topic.name))),
          handler).use{ _ =>
          for {
            recordPartition <- UIO(ProducerRecord(topic.name, Chunk.empty, partition = Some(0)))
            _ <- producer.produce(recordPartition)
            awaitResult <- calledGroupsTopicsAfterAssignment.await.timeout(fromScala(10.seconds))
            groupTopics <- groupTopicsRef.get
          } yield (awaitResult, groupTopics)
        }
      } yield {
        (awaitResult aka "awaitResult" must not(beNone)) and
        (groupTopics === Map(group -> Set(topic.name)))
      }
    }
  }

  private def aTopicConfig(name: String = s"topic-${System.currentTimeMillis}") =
    TopicConfig(name, 1, 1, Delete(1.hour.toMillis))
}
