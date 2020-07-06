package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.{Group, Headers, Topic}
import zio.clock.Clock
import zio.duration.Duration
import zio.{Chunk, UIO, URIO}

// TODO: move to 'retry' package
object BlockingAndNonBlockingRetryPolicy {
  def blockingThenNonBlocking(group: Group, blockingBackoffs: Seq[Duration], nonBlockingBackoffs: Seq[Duration]): RetryPolicy =
    new RetryPolicy {
      val nonBlockingRetryPolicy = NonBlockingRetryPolicy.defaultNonBlocking(group, backoffs = nonBlockingBackoffs:_*)

      override def retryTopicsFor(originalTopic: Topic): Set[Topic] = nonBlockingRetryPolicy.retryTopicsFor(originalTopic)

      override def retryAttempt(topic: Topic, headers: Headers, subscription: ConsumerSubscription): UIO[Option[RetryAttempt]] =
        nonBlockingRetryPolicy.retryAttempt(topic, headers, subscription)

      override def retryDecision[E](retryAttempt: Option[RetryAttempt], record: ConsumerRecord[Chunk[Byte], Chunk[Byte]], error: E, subscription: ConsumerSubscription): URIO[Clock, RetryDecision] =
        nonBlockingRetryPolicy.retryDecision(retryAttempt, record, error, subscription)

      override def blockingRetries: BlockingRetries = BlockingRetriesFollowedByNonBlocking(blockingBackoffs)
    }
}