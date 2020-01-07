package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.testkit.BaseTest
import org.apache.kafka.common.TopicPartition
import zio.{Managed, UManaged}

class OffsetsTest extends BaseTest[Any] {

  override def env: UManaged[Any] = Managed.unit

  "get current offsets" in {
    for {
      offsets <- Offsets.make
      topicPartition = new TopicPartition("topic", 0)
      _ <- offsets.update(topicPartition, 0L)
      current <- offsets.getAndClear
    } yield current must havePair(topicPartition -> 0L)
  }

  "clear offsets" in {
    for {
      offsets <- Offsets.make
      _ <- offsets.update(new TopicPartition("topic", 0), 0L)
      _ <- offsets.getAndClear
      current <- offsets.getAndClear
    } yield current must beEmpty
  }

  "update with larger offset" in {
    val partition0 = new TopicPartition("topic", 0)
    val partition1 = new TopicPartition("topic", 1)

    for {
      offsets <- Offsets.make
      _ <- offsets.update(partition0, 1L)
      _ <- offsets.update(partition0, 0L)
      _ <- offsets.update(partition1, 0L)
      current <- offsets.getAndClear
    } yield current must havePairs(partition0 -> 1L, partition1 -> 0L)
  }

}
