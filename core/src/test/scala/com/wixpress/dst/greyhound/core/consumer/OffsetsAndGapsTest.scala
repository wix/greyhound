package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.TopicPartition
import com.wixpress.dst.greyhound.core.consumer.Gap.GAP_SEPARATOR
import com.wixpress.dst.greyhound.core.consumer.OffsetAndGaps.LAST_HANDLED_OFFSET_SEPARATOR
import com.wixpress.dst.greyhound.core.consumer.OffsetGapsTest._
import com.wixpress.dst.greyhound.core.testkit.BaseTestNoEnv

class OffsetsAndGapsTestGapsTest extends BaseTestNoEnv {

  "calculate gaps created by handled batch" in {
    for {
      offsetGaps  <- OffsetsAndGaps.make
      _           <- offsetGaps.update(topicPartition, Seq(1L, 3L, 7L))
      currentGaps <- offsetGaps.gapsForPartition(topicPartition)
    } yield currentGaps must beEqualTo(Seq(Gap(0L, 0L), Gap(2L, 2L), Gap(4L, 6L)))
  }

  "update offset and gaps according to handled batch" in {
    for {
      offsetGaps             <- OffsetsAndGaps.make
      _                      <- offsetGaps.update(topicPartition, Seq(1L, 3L, 7L))
      _                      <- offsetGaps.update(topicPartition, Seq(2L, 5L))
      getCommittableAndClear <- offsetGaps.getCommittableAndClear
    } yield getCommittableAndClear must havePair(topicPartition -> OffsetAndGaps(7L, Seq(Gap(0L, 0L), Gap(4L, 4L), Gap(6L, 6L))))
  }

  "clear committable offsets" in {
    for {
      offsetGaps             <- OffsetsAndGaps.make
      _                      <- offsetGaps.update(topicPartition, Seq(1L, 3L, 7L))
      _                      <- offsetGaps.getCommittableAndClear
      getCommittableAndClear <- offsetGaps.getCommittableAndClear
    } yield getCommittableAndClear must beEmpty
  }

  "do not clear gaps on retrieving current" in {
    for {
      offsetGaps  <- OffsetsAndGaps.make
      _           <- offsetGaps.update(topicPartition, Seq(1L, 3L, 7L))
      _           <- offsetGaps.gapsForPartition(topicPartition)
      currentGaps <- offsetGaps.gapsForPartition(topicPartition)
    } yield currentGaps must beEqualTo(Seq(Gap(0L, 0L), Gap(2L, 2L), Gap(4L, 6L)))
  }

  "update with larger offset" in {
    val partition0 = TopicPartition(topic, 0)
    val partition1 = TopicPartition(topic, 1)

    for {
      offsetGaps <- OffsetsAndGaps.make
      _          <- offsetGaps.update(partition0, Seq(1L))
      _          <- offsetGaps.update(partition0, Seq(0L))
      _          <- offsetGaps.update(partition1, Seq(0L))
      current    <- offsetGaps.getCommittableAndClear
    } yield current must havePairs(partition0 -> OffsetAndGaps(1L, Seq()), partition1 -> OffsetAndGaps(0L, Seq()))
  }

  "init with given offsets and calculate subsequent gaps accordingly" in {
    val partition0              = TopicPartition(topic, 0)
    val partition1              = TopicPartition(topic, 1)
    val initialCommittedOffsets =
      Map(partition0 -> OffsetAndGaps(100L, committable = false), partition1 -> OffsetAndGaps(200L, committable = false))
    for {
      offsetGaps <- OffsetsAndGaps.make
      _          <- offsetGaps.init(initialCommittedOffsets)
      _          <- offsetGaps.update(partition0, Seq(101L, 102L))
      _          <- offsetGaps.update(partition1, Seq(203L, 204L))
      current    <- offsetGaps.getCommittableAndClear
    } yield current must havePairs(partition0 -> OffsetAndGaps(102L, Seq()), partition1 -> OffsetAndGaps(204L, Seq(Gap(201L, 202L))))
  }

  "parse gaps from string" in {
    val gaps     = Seq(s"10${LAST_HANDLED_OFFSET_SEPARATOR}0${GAP_SEPARATOR}1", s"10${LAST_HANDLED_OFFSET_SEPARATOR}", "")
    val expected = Seq(Some(OffsetAndGaps(10, Seq(Gap(0, 1)))), Some(OffsetAndGaps(10, Seq())), None)
    gaps.map(OffsetsAndGaps.parseGapsString).must(beEqualTo(expected))
  }

}

object OffsetGapsTest {
  val topic          = "some-topic"
  val topicPartition = TopicPartition(topic, 0)
}
