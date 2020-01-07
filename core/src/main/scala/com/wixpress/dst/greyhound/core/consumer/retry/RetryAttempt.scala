package com.wixpress.dst.greyhound.core.consumer.retry

import java.time
import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import com.wixpress.dst.greyhound.core.consumer.retry.RetryAttempt.{RetryAttemptNumber, currentTime}
import zio.clock.Clock
import zio.duration.Duration
import zio.{URIO, clock}

case class RetryAttempt(attempt: RetryAttemptNumber,
                        submittedAt: Instant,
                        backoff: Duration) {

  def sleep: URIO[Clock, Unit] = currentTime.flatMap { now =>
    val expiresAt = submittedAt.plusMillis(backoff.toMillis)
    val sleep = time.Duration.between(now, expiresAt)
    clock.sleep(Duration.fromJava(sleep))
  }

}

object RetryAttempt {
  type RetryAttemptNumber = Int

  val currentTime = clock.currentTime(MILLISECONDS).map(Instant.ofEpochMilli)
}
