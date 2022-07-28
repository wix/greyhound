package com.wixpress.dst.greyhound.core.consumer.batched

import com.wixpress.dst.greyhound.core.consumer.batched.BatchEventLoop.Record
import zio.test.Assertion._
import zio.test._
import zio.test.junit.JUnitRunnableSpec

class BatchEventLoopStateTest extends JUnitRunnableSpec {
  private implicit val  trace = zio.Trace.empty
  def spec = suite("BatchEventLoopState")(
    test("pause, resume, shutdown") {
      for {
        state              <- BatchEventLoopState.make
        initialIsRunning   <- state.isRunning
        initialNotShutdown <- state.notShutdown
        _                  <- state.pause()
        runningAfterPause  <- state.isRunning
        _                  <- state.resume()
        runningAfterResume <- state.isRunning
        _                  <- state.shutdown()
        notShutDownAfter   <- state.notShutdown
      } yield {
        assert(initialIsRunning)(isTrue) && assert(runningAfterPause)(isFalse) && assert(runningAfterResume)(isTrue) &&
        assert(initialNotShutdown)(isTrue) && assert(notShutDownAfter)(isFalse)
      }
    },
    test("pause and resume partitions") {
      val records1, records2 = TestSupport.records(topicCount = 1, partitions = 1)
      val (tp1, tp2)         = (records1.head.topicPartition, records2.head.topicPartition)
      for {
        state        <- BatchEventLoopState.make
        _            <- state.attemptFailed(tp1, records1)
        pauseResume1 <- state.shouldPauseAndResume()
        _            <- state.partitionsPausedResumed(pauseResume1)
        exposed1     <- state.eventLoopState
        _            <- state.clearPending(Set(tp1))
        _            <- state.attemptFailed(tp2, records2)
        pauseResume2 <- state.shouldPauseAndResume()
        _            <- state.partitionsPausedResumed(pauseResume2)
        exposed2     <- state.eventLoopState
      } yield {
        assert(pauseResume1.toPause)(equalTo(Set(tp1)).label("pauseResume1.toPause")) && assert(pauseResume1.toResume)(isEmpty) &&
        assert(exposed1.pausedPartitions)(equalTo(Set(tp1))) &&
        // after tp1 was cleared but tp2 failed
        assert(pauseResume2.toPause)(equalTo(Set(tp2)).label("pauseResume2.toPause")) && assert(pauseResume2.toResume)(equalTo(Set(tp1))) &&
        assert(exposed2.pausedPartitions)(equalTo(Set(tp2)).label("elState2.pausedPartitions"))
      }
    },
    test("revoke partition") {
      val records1, records2, records3 = TestSupport.records(topicCount = 1, partitions = 1)
      val (tp1, tp2, tp3)              = (records1.head.topicPartition, records2.head.topicPartition, records3.head.topicPartition)
      for {
        state   <- BatchEventLoopState.make
        _       <- state.attemptFailed(tp1, records1) *> state.attemptFailed(tp2, records2) *> state.attemptFailed(tp3, records3)
        _       <- state.partitionsPaused(Set(tp1, tp2, tp3))
        _       <- state.partitionsRevoked(Set(tp1, tp2))
        exposed <- state.eventLoopState
      } yield {
        assert(exposed.pausedPartitions)(equalTo(Set(tp3)).label("pausedPartitions")) &&
        assert(exposed.pendingRecords)(equalTo(Map(tp3 -> records3.size)).label("pendingRecords"))
      }
    }
  )

  implicit class RecordsOps(val records: Seq[Record]) {
    def only(partition: Int)   = records.filter(_.partition == partition)
    def except(partition: Int) = records.filterNot(_.partition == partition)
  }
}
