package com.wixpress.dst.greyhound.core.consumer

import java.time.{Clock, Duration, ZoneId}
import java.util.concurrent.atomic.AtomicReference

import com.wixpress.dst.greyhound.core.TopicPartition
import com.wixpress.dst.greyhound.core.consumer.ConsumerMetric.CommittedMissingOffsets
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import org.specs2.mock.Mockito
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.Scope

import scala.util.Random

class MissingOffsetsCommitterTest extends SpecificationWithJUnit with Mockito {

  "do nothing if no missing offsets" in new ctx {
    givenCommittedOffsets(partitions)(randomOffsets(partitions))

    committer.commitMissingOffsets(partitions)

    there was no(offsetOps).position(any(), any())
    there was no(offsetOps).commit(any(), any())
  }

  "commit missing offsets" in new ctx {
    givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
    val p2Pos, p3Pos = randomInt.toLong
    givenPositions(p2 -> p2Pos, p3 -> p3Pos)

    committer.commitMissingOffsets(partitions)

    val missingOffsets = Map(
      p2 -> p2Pos,
      p3 -> p3Pos
    )
    there was one(offsetOps).commit(
      missingOffsets,
      timeout
    )

    reported must contain(CommittedMissingOffsets(clientId, group, partitions, missingOffsets, elapsed = Duration.ZERO))
  }

  "report errors in `committed()`, but not fail" in new ctx {
    val e = new RuntimeException(randomStr)
    offsetOps.committed(any(), any()) throws e

    committer.commitMissingOffsets(partitions)

    reported must
      contain(CommittedMissingOffsets(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, Some(e)))
  }

  "report errors in `commit()`, but not fail" in new ctx {
    val e = new RuntimeException(randomStr)
    givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
    val p2Pos, p3Pos = randomInt.toLong
    givenPositions(p2 -> p2Pos, p3 -> p3Pos)
    offsetOps.commit(any(), any()) throws e

    committer.commitMissingOffsets(partitions)

    reported must
      contain(CommittedMissingOffsets(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, Some(e)))
  }

  "report errors in `commit()`, but not fail" in new ctx {
    val e = new RuntimeException(randomStr)
    givenCommittedOffsets(partitions)(Map(p1 -> randomInt))
    offsetOps.position(any(), any()) throws e

    committer.commitMissingOffsets(partitions)

    reported must
      contain(CommittedMissingOffsets(clientId, group, partitions, Map.empty, elapsed = Duration.ZERO, Some(e)))
  }

  trait ctx extends Scope {
    private val metricsLogRef = new AtomicReference(Seq.empty[GreyhoundMetric])
    def reported = metricsLogRef.get
    val timeout = Duration.ofMillis(123)
    def report(metric: GreyhoundMetric) = metricsLogRef.updateAndGet(_ :+ metric)
    val offsetOps = mock[UnsafeOffsetOperations]
    val group, clientId = randomStr
    val p1, p2, p3 = TopicPartition(randomStr, randomInt)
    val partitions = Set(p1, p2, p3)
    val clock = Clock.fixed(java.time.Instant.EPOCH, ZoneId.of("UTC"))

    val committer = new MissingOffsetsCommitter(clientId, group, offsetOps, timeout, report, clock)

    def randomOffsets(partitions: Set[TopicPartition]) = partitions.map(p => p -> randomInt.toLong).toMap

    def randomStr = Random.alphanumeric.take(5).mkString
    def randomInt = Random.nextInt(200)

    def givenCommittedOffsets(partitions: Set[TopicPartition])(result: Map[TopicPartition, Long]) = {
      offsetOps.committed(partitions, timeout) returns result
    }


    def givenPositions(positions: (TopicPartition, Long)*) = {
      positions.foreach { case (tp, p) =>
        offsetOps.position(tp, timeout) returns p
      }
    }
  }
}
