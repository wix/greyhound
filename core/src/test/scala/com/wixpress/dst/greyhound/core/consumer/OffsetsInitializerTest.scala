package com.wixpress.dst.greyhound.core.consumer

import java.time.{Clock, Duration, ZoneId}
import java.util.concurrent.atomic.AtomicReference
import com.wixpress.dst.greyhound.core.TopicPartition
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
        one(offsetOps).commit(
          missingOffsets,
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
        one(offsetOps).commit(
          expected,
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
        one(offsetOps).commit(
          expected,
          timeoutIfSeek
        )
      there was one(offsetOps).seek(Map(p1 -> p1Pos, p2 -> p2Pos))

      there was no(offsetOps).position(===(p1), any())

      reported must contain(CommittedMissingOffsets(clientId, group, partitions, expected, elapsed = Duration.ZERO, expected))
    }

  "fail if operation fails and there are relevant seekTo offsets" in
    new ctx(seekTo = Map(p1 -> SeekToOffset(p1Pos))) {
      val e = new RuntimeException(randomStr)
      offsetOps.committed(any(), any()) throws e

      committer.initializeOffsets(partitions) must throwA(e)

      reported must contain(CommittedMissingOffsetsFailed(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, e))
    }

  "report errors in `commit()`, but not fail" in
    new ctx {
      val e            = new RuntimeException(randomStr)
      givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
      val p2Pos, p3Pos = randomInt.toLong
      givenPositions(p2 -> p2Pos, p3 -> p3Pos)
      offsetOps.commit(any(), any()) throws e

      committer.initializeOffsets(partitions)

      reported must contain(CommittedMissingOffsetsFailed(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, e))
    }

  "report errors in `commit()`, but not fail" in
    new ctx {
      val e = new RuntimeException(randomStr)
      givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
      offsetOps.position(any(), any()) throws e

      committer.initializeOffsets(partitions)

      reported must contain(CommittedMissingOffsetsFailed(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, e))
    }

  abstract class ctx(val seekTo: Map[TopicPartition, SeekTo] = Map.empty) extends Scope {
    private val metricsLogRef           = new AtomicReference(Seq.empty[GreyhoundMetric])
    def reported                        = metricsLogRef.get
    val timeout                         = Duration.ofMillis(123)
    val timeoutIfSeek                   = Duration.ofMillis(231)
    def report(metric: GreyhoundMetric) = metricsLogRef.updateAndGet(_ :+ metric)
    val offsetOps                       = mock[UnsafeOffsetOperations]
    val group, clientId                 = randomStr

    val clock = Clock.fixed(java.time.Instant.EPOCH, ZoneId.of("UTC"))

    val committer = new OffsetsInitializer(
      clientId,
      group,
      offsetOps,
      timeout,
      timeoutIfSeek,
      report,
      if (seekTo == Map.empty) InitialOffsetsSeek.default else (_, _, _, _) => seekTo,
      clock
    )

    def randomOffsets(partitions: Set[TopicPartition]) = partitions.map(p => p -> randomInt.toLong).toMap

    def givenCommittedOffsets(partitions: Set[TopicPartition], timeout: zio.duration.Duration = timeout)(
      result: Map[TopicPartition, Long]
    ) = {
      offsetOps.committed(partitions, timeout) returns result
    }

    def givenEndOffsets(partitions: Set[TopicPartition], timeout: zio.duration.Duration = timeout)(result: Map[TopicPartition, Long]) = {
      offsetOps.endOffsets(partitions, timeout) returns result
    }

    offsetOps.beginningOffsets(partitions, timeout) returns Map.empty
    offsetOps.endOffsets(partitions, timeout) returns Map.empty
    offsetOps.beginningOffsets(partitions, timeoutIfSeek) returns Map.empty
    offsetOps.endOffsets(partitions, timeoutIfSeek) returns Map.empty

    def givenPositions(positions: (TopicPartition, Long)*): Unit = {
      givenPositions(timeout, positions: _*)
    }

    def givenPositions(timeout: zio.duration.Duration, positions: (TopicPartition, Long)*): Unit = {
      positions.foreach {
        case (tp, p) =>
          offsetOps.position(tp, timeout) returns p
      }
    }

  }

  private def randomStr       = Random.alphanumeric.take(5).mkString
  private def randomInt       = Random.nextInt(200)
  private def randomPartition = TopicPartition(randomStr, randomInt)
}
