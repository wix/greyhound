package com.wixpress.dst.greyhound.core.consumer

import java.time.{Clock, Duration, ZoneId}
import java.util.concurrent.atomic.AtomicReference
import com.wixpress.dst.greyhound.core.{OffsetAndMetadata, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.{CommittedMissingOffsets, CommittedMissingOffsetsFailed}
import com.wixpress.dst.greyhound.core.consumer.SeekTo.{SeekToEnd, SeekToOffset}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

import scala.util.Random

class OffsetsInitializerTest extends SpecificationWithJUnit with Mockito {
  private val Seq(p1, p2, p3)     = Seq("t1" -> 1, "t2" -> 2, "t3" -> 3).map(tp => TopicPartition(tp._1, tp._2))
  private val partitions          = Set(p1, p2, p3)
  private val p1Pos, p2Pos, p3Pos = randomInt.toLong
  val epochTimeToRewind           = 1000L

  "do nothing if no missing offsets" in
    new ctx {
      givenCommittedOffsets(partitions)(randomOffsets(partitions))

      committer.initializeOffsets(partitions)

      there was no(offsetOps).position(any(), any())
      there was no(offsetOps).commit(any(), any())
    }

  "commit missing offsets" in
    new ctx {
      givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
      givenPositions(p2 -> p2Pos, p3 -> p3Pos)

      committer.initializeOffsets(partitions)

      val missingOffsets = Map(
        p2 -> p2Pos,
        p3 -> p3Pos
      )
      there was
        one(offsetOps).commitWithMetadata(
          missingOffsets.mapValues(OffsetAndMetadata(_)),
          timeout
        )

      reported must contain(CommittedMissingOffsets(clientId, group, partitions, missingOffsets, elapsed = Duration.ZERO, missingOffsets))
    }

  "seek to specified offsets if nothing committed" in
    new ctx(seekTo = Map(p1 -> SeekToOffset(p1Pos))) {
      givenCommittedOffsets(partitions, timeoutIfSeek)(Map(p2 -> p2Pos))
      givenPositions(timeoutIfSeek, p1 -> (p1Pos + 100), p3 -> p3Pos)

      committer.initializeOffsets(partitions)

      val expected = Map(p1 -> p1Pos, p3 -> p3Pos)
      there was
        one(offsetOps).commitWithMetadata(
          expected.mapValues(OffsetAndMetadata(_)),
          timeoutIfSeek
        )

      there was one(offsetOps).seek(Map(p1 -> p1Pos))

      there was no(offsetOps).position(===(p1), any())

      reported must contain(CommittedMissingOffsets(clientId, group, partitions, expected, elapsed = Duration.ZERO, expected))
    }

  "seek to specified offsets" in
    new ctx(seekTo = Map(p1 -> SeekToOffset(p1Pos), p2 -> SeekToEnd)) {
      givenCommittedOffsets(partitions, timeoutIfSeek)(Map(p1 -> (p1Pos - 1), p2 -> randomInt))
      givenEndOffsets(Set(p2), timeoutIfSeek)(Map(p2 -> p2Pos))
      givenPositions(timeoutIfSeek, p3 -> p3Pos)

      committer.initializeOffsets(partitions)

      val expected = Map(p1 -> p1Pos, p3 -> p3Pos, p2 -> p2Pos)
      there was
        one(offsetOps).commitWithMetadata(
          expected.mapValues(OffsetAndMetadata(_)),
          timeoutIfSeek
        )
      there was one(offsetOps).seek(Map(p1 -> p1Pos, p2 -> p2Pos))

      there was no(offsetOps).position(===(p1), any())

      reported must contain(CommittedMissingOffsets(clientId, group, partitions, expected, elapsed = Duration.ZERO, expected))
    }

  "fail if operation fails and there are relevant seekTo offsets" in
    new ctx(seekTo = Map(p1 -> SeekToOffset(p1Pos))) {
      val e = new RuntimeException(randomStr)
      offsetOps.committedWithMetadata(any(), any()) throws e

      committer.initializeOffsets(partitions) must throwA(e)

      reported must contain(CommittedMissingOffsetsFailed(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, e))
    }

  "report errors in `commitWithMetadata()`, but not fail" in
    new ctx {
      val e            = new RuntimeException(randomStr)
      givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
      val p2Pos, p3Pos = randomInt.toLong
      givenPositions(p2 -> p2Pos, p3 -> p3Pos)
      offsetOps.commitWithMetadata(any(), any()) throws e

      committer.initializeOffsets(partitions)

      reported must contain(CommittedMissingOffsetsFailed(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, e))
    }

  "report errors in `commitWithMetadata()`, but not fail" in
    new ctx {
      val e = new RuntimeException(randomStr)
      givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
      offsetOps.position(any(), any()) throws e

      committer.initializeOffsets(partitions)

      reported must contain(CommittedMissingOffsetsFailed(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, e))
    }

  "rewind uncommitted offsets" in
    new ctx {
      givenCommittedOffsets(partitions)(Map(p2 -> randomInt))
      givenPositions(p2                          -> p2Pos, p3 -> p3Pos)
      givenOffsetsForTimes(epochTimeToRewind, p1 -> 0L, p2    -> 1L)

      committer.initializeOffsets(partitions)

      val missingOffsets = Map(
        p1 -> p1Pos,
        p3 -> p3Pos
      )

      val rewindedOffsets = Map(
        p1 -> 0L
      )

      there was
        one(offsetOps).commitWithMetadata(
          (missingOffsets ++ rewindedOffsets).mapValues(OffsetAndMetadata(_)),
          timeout
        )
    }

  "rewind to endOffsets for uncommitted partitions when offsetsForTimes return null offsets " in
    new ctx {
      givenCommittedOffsets(partitions)(Map(p2 -> randomInt, p3 -> randomInt))
      givenPositions(p3                -> p3Pos)
      givenEndOffsets(partitions, timeout)(Map(p1 -> p1Pos))
      givenOffsetsForTimes(Set(p1))(p1 -> None /*kafka SDK returned null*/ )

      committer.initializeOffsets(partitions)

      val committedOffsets = Map(
        p1 -> p1Pos
      )

      there was
        one(offsetOps).commitWithMetadata(
          committedOffsets.mapValues(OffsetAndMetadata(_)),
          timeout
        )
    }

  "not rewind uncommitted offsets when offset reset is earliest" in
    new ctx(offsetReset = OffsetReset.Earliest) {
      givenCommittedOffsets(partitions)(Map(p2 -> randomInt))
      givenPositions(p2                          -> p2Pos, p3 -> p3Pos)
      givenOffsetsForTimes(epochTimeToRewind, p1 -> 0L, p2    -> 1L)

      committer.initializeOffsets(partitions)

      val missingOffsets = Map(
        p1 -> p1Pos,
        p3 -> p3Pos
      )

      val rewindedOffsets = Map(
        p1 -> 0L
      )

      there was
        one(offsetOps).commitWithMetadata(
          (missingOffsets ++ rewindedOffsets).mapValues(OffsetAndMetadata(_)),
          timeout
        )
    }

  class ctx(val seekTo: Map[TopicPartition, SeekTo] = Map.empty, offsetReset: OffsetReset = OffsetReset.Latest) extends Scope {
    private val metricsLogRef           = new AtomicReference(Seq.empty[GreyhoundMetric])
    def reported                        = metricsLogRef.get
    val timeout                         = Duration.ofMillis(123)
    val timeoutIfSeek                   = Duration.ofMillis(231)
    def report(metric: GreyhoundMetric) = metricsLogRef.updateAndGet(_ :+ metric)
    val offsetOps                       = mock[UnsafeOffsetOperations]
    val group, clientId                 = randomStr

    givenNoOffsetsForTimes
    val clock = Clock.fixed(java.time.Instant.EPOCH, ZoneId.of("UTC"))

    val committer = new OffsetsInitializer(
      clientId,
      group,
      offsetOps,
      timeout,
      timeoutIfSeek,
      report,
      if (seekTo == Map.empty) InitialOffsetsSeek.default else (_, _, _, _) => seekTo,
      rewindUncommittedOffsetsBy = zio.Duration.fromMillis(15 * 60 * 1000),
      clock,
      offsetResetIsEarliest = offsetReset == OffsetReset.Earliest,
      false
    )

    def randomOffsets(partitions: Set[TopicPartition]) = partitions.map(p => p -> randomInt.toLong).toMap

    def givenCommittedOffsets(partitions: Set[TopicPartition], timeout: zio.Duration = timeout)(
      result: Map[TopicPartition, Long]
    ) = {
      offsetOps.committedWithMetadata(partitions, timeout) returns result.mapValues(OffsetAndMetadata(_))
    }

    def givenEndOffsets(partitions: Set[TopicPartition], timeout: zio.Duration = timeout)(result: Map[TopicPartition, Long]) = {
      offsetOps.endOffsets(partitions, timeout) returns result
    }

    offsetOps.beginningOffsets(partitions, timeout) returns Map.empty
    offsetOps.endOffsets(partitions, timeout) returns Map.empty
    offsetOps.beginningOffsets(partitions, timeoutIfSeek) returns Map.empty
    offsetOps.endOffsets(partitions, timeoutIfSeek) returns Map.empty

    def givenPositions(positions: (TopicPartition, Long)*): Unit = {
      givenPositions(timeout, positions: _*)
    }

    def givenPositions(timeout: zio.Duration, positions: (TopicPartition, Long)*): Unit = {
      positions.foreach {
        case (tp, p) =>
          offsetOps.position(tp, timeout) returns p
      }
    }

    def givenNoOffsetsForTimes: Unit =
      offsetOps.offsetsForTimes(anyObject, anyObject, anyObject) returns Map.empty

    def givenOffsetsForTimes(epochTime: Long, positions: (TopicPartition, Long)*): Unit =
      offsetOps.offsetsForTimes(`===`(positions.toMap.keySet), `===`(epochTime), anyObject) returns positions.toMap.mapValues(Option(_))

    def givenOffsetsForTimes(partitions: Set[TopicPartition])(positions: (TopicPartition, Option[Long])*): Unit =
      offsetOps.offsetsForTimes(`===`(partitions), anyObject, anyObject) returns positions.toMap
  }

  private def randomStr       = Random.alphanumeric.take(5).mkString
  private def randomInt       = Random.nextInt(200)
  private def randomPartition = TopicPartition(randomStr, randomInt)
}
