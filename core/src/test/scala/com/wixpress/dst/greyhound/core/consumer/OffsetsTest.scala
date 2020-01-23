package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.consumer.Offsets.WaitForOffsets
import com.wixpress.dst.greyhound.core.consumer.OffsetsTest._
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import zio.clock.Clock
import zio.duration._
import zio.{UManaged, ZManaged}

class OffsetsTest extends BaseTest[Clock] {

  override def env: UManaged[Clock] =
    ZManaged.succeed(Clock.Live)

  "get current offsets" in {
    for {
      offsets <- Offsets.make
      topicPartition = TopicPartition(topic, 0)
      _ <- offsets.update(topicPartition, 0L)
      committable <- offsets.committable
    } yield committable must havePair(topicPartition -> 0L)
  }

  "clear committable offsets" in {
    for {
      offsets <- Offsets.make
      _ <- offsets.update(TopicPartition(topic, 0), 0L)
      _ <- offsets.committable
      committable <- offsets.committable
    } yield committable must beEmpty
  }

  "update with larger offset" in {
    val partition0 = TopicPartition(topic, 0)
    val partition1 = TopicPartition(topic, 1)

    for {
      offsets <- Offsets.make
      _ <- offsets.update(partition0, 1L)
      _ <- offsets.update(partition0, 0L)
      _ <- offsets.update(partition1, 0L)
      current <- offsets.committable
    } yield current must havePairs(partition0 -> 1L, partition1 -> 0L)
  }

  "wait for offsets" should {
    "return immediately if map is empty" in {
      for {
        offsets <- Offsets.make
        result <- offsets.waitFor(Map.empty).timeout(100.millis)
      } yield result must beSome
    }

    "wait until the requested offsets have reached" in {
      val partition = TopicPartition(topic, 0)

      for {
        offsets <- Offsets.make
        result1 <- offsets.waitFor(Map(partition -> 1L)).timeout(100.millis)
        fiber <- offsets.waitFor(Map(partition -> 1L)).fork
        _ <- offsets.update(partition, 1L)
        result2 <- fiber.join.timeout(100.millis)
      } yield (result1 must beNone) and (result2 must beSome)
    }

    "consider both committed and committable offsets" in {
      val partition = TopicPartition(topic, 0)

      for {
        offsets <- Offsets.make
        result1 <- offsets.waitFor(Map(partition -> 1L)).timeout(100.millis)
        _ <- offsets.update(partition, 1L)
        _ <- offsets.committable
        result2 <- offsets.waitFor(Map(partition -> 1L)).timeout(100.millis)
      } yield (result1 must beNone) and (result2 must beSome)
    }

    "isReady" should {
      val minimumOffsets = Map(
        TopicPartition(topic, 0) -> 0L,
        TopicPartition(topic, 1) -> 1L)

      "return true if the offsets maps match" in {
        WaitForOffsets.isReady(minimumOffsets, minimumOffsets) must beTrue
      }

      "return true if the current offsets are ahead of minimum offsets" in {
        val currentOffsets = Map(
          TopicPartition(topic, 0) -> 5L,
          TopicPartition(topic, 1) -> 5L)

        WaitForOffsets.isReady(minimumOffsets, currentOffsets) must beTrue
      }

      "return false if at least one partition is behind" in {
        val currentOffsets = Map(
          TopicPartition(topic, 0) -> 5L,
          TopicPartition(topic, 1) -> 0L)

        WaitForOffsets.isReady(minimumOffsets, currentOffsets) must beFalse
      }

      "return false if a required partition is missing" in {
        val currentOffsets = Map(TopicPartition(topic, 1) -> 5L)

        WaitForOffsets.isReady(minimumOffsets, currentOffsets) must beFalse
      }
    }
  }

}

object OffsetsTest {
  val topic = "topic"
}
