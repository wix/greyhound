package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.TopicPartition
import com.wixpress.dst.greyhound.core.consumer.OffsetsTest._
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import zio.{UManaged, ZManaged}

class OffsetsTest extends BaseTest[Any] {

  override def env: UManaged[Any] = ZManaged.unit

  "get current offsets" in {
    for {
      offsets       <- Offsets.make
      topicPartition = TopicPartition(topic, 0)
      _             <- offsets.update(topicPartition, 0L)
      committable   <- offsets.committable
    } yield committable must havePair(topicPartition -> 0L)
  }

  "clear committable offsets" in {
    for {
      offsets     <- Offsets.make
      _           <- offsets.update(TopicPartition(topic, 0), 0L)
      _           <- offsets.committable
      committable <- offsets.committable
    } yield committable must beEmpty
  }

  "update with larger offset" in {
    val partition0 = TopicPartition(topic, 0)
    val partition1 = TopicPartition(topic, 1)

    for {
      offsets <- Offsets.make
      _       <- offsets.update(partition0, 1L)
      _       <- offsets.update(partition0, 0L)
      _       <- offsets.update(partition1, 0L)
      current <- offsets.committable
    } yield current must havePairs(partition0 -> 1L, partition1 -> 0L)
  }

}

object OffsetsTest {
  val topic = "topic"
}
