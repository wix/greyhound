package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer._
import zio.clock.Clock
import zio.duration._
import zio.{Chunk, UIO, URIO}

case class FakeBlockingRetryPolicy(_intervals: Duration*) extends RetryPolicy {

  override def retryTopicsFor(originalTopic: Topic): Set[Topic] = Set.empty

  override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]] =
    UIO(None)

  override def retryDecision[E](retryAttempt: Option[RetryAttempt],
                                record: ConsumerRecord[Chunk[Byte], Chunk[Byte]],
                                error: E,
                                subscription: ConsumerSubscription): URIO[Clock, RetryDecision] =
    ???

  override def intervals: Seq[Duration] = _intervals
}