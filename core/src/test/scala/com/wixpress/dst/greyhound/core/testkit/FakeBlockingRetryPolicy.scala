package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.consumer._
import zio.duration._

case class FakeBlockingRetryPolicy(intervals: Duration*) extends NoOpNonBlockingRetryPolicy {
  override def blockingIntervals: Seq[Duration] = intervals
}

case class FakeInfiniteBlockingRetryPolicy(interval: Duration) extends NoOpNonBlockingRetryPolicy {
  override def blockingIntervals: Seq[Duration] = Stream.continually(interval)
}