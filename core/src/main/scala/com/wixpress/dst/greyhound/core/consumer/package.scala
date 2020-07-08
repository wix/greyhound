package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import zio.{RIO, ZIO}
import zio.blocking.Blocking

package object consumer {
  def subscribe[R](subscription: ConsumerSubscription, listener: RebalanceListener[R])(consumer: Consumer): RIO[Blocking with GreyhoundMetrics with R, Unit] =
    ZIO.whenCase(subscription) {
      case TopicPattern(pattern, _) =>
        consumer.subscribePattern(pattern, listener)
      case Topics(topics) =>
        consumer.subscribe(topics, listener)
    }
}