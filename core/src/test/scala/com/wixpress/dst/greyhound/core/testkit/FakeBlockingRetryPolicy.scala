package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.consumer._
import zio.duration._

case class FakeBlockingRetryPolicy(intervals: Duration*) extends NoOpNonBlockingRetryPolicy {
  override def blockingRetries: BlockingRetries = FiniteBlockingRetriesOnly(intervals)
}

case class FakeInfiniteBlockingRetryPolicy(interval: Duration) extends NoOpNonBlockingRetryPolicy {
  override def blockingRetries: BlockingRetries = InfiniteBlockingRetriesOnly(interval)
}

case class FakeBlockingAndNonBlockingRetryPolicy(topic: Topic, intervals: Duration*) extends FakeNonBlockingRetryPolicy {
  override def blockingRetries: BlockingRetries = BlockingRetriesFollowedByNonBlocking(intervals)
}