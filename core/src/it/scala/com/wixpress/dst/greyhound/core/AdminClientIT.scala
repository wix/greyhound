package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.CleanupPolicy.Delete
import com.wixpress.dst.greyhound.core.testkit.BaseTestWithSharedEnv
import com.wixpress.dst.greyhound.testkit.ITEnv
import com.wixpress.dst.greyhound.testkit.ITEnv.{Env, TestResources, testResources}
import org.apache.kafka.common.errors.InvalidTopicException
import zio.UManaged

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
  }

  private def aTopicConfig(name: String = s"topic-${System.currentTimeMillis}") =
    TopicConfig(name, 1, 1, Delete(1.hour.toMillis))
}
