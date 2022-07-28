package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription
import com.wixpress.dst.greyhound.core.consumer.{OffsetReset, RecordConsumer, RecordConsumerConfig}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{ClientId, Group, NonEmptySet, Topic}
import com.wixpress.dst.greyhound.future.GreyhoundConsumer.Handler
import com.wixpress.dst.greyhound.future.GreyhoundRuntime.Env
import zio._

import scala.concurrent.Future

trait GreyhoundConsumers extends Closeable {
  def pause: Future[Unit]

  def resume: Future[Unit]

  def isAlive: Future[Boolean]
}

case class GreyhoundConsumersBuilder(
  config: GreyhoundConfig,
  handlers: Map[(Group, ClientId), (OffsetReset, NonEmptySet[Topic], Handler, RecordConsumerConfig => RecordConsumerConfig)] = Map.empty
) {

  def withConsumer[K, V](consumer: GreyhoundConsumer[K, V]): GreyhoundConsumersBuilder =
    copy(handlers =
      handlers +
        ((consumer.group, consumer.clientId) ->
          (consumer.offsetReset, consumer.initialTopics, consumer.recordHandler, consumer.mutateConsumerConfig))
    )

  def build: Future[GreyhoundConsumers] = config.runtime.unsafeRunToFuture {
    for {
      consumers <- ZIO.foreach(handlers.toSeq) {
                       case ((group, clientId), (offsetReset, initialTopics, handler, mutateConsumerConfig)) =>
                         RecordConsumer.make(
                           handler = handler,
                           config = mutateConsumerConfig(
                             RecordConsumerConfig(
                               config.bootstrapServers,
                               group,
                               ConsumerSubscription.Topics(initialTopics),
                               offsetReset = offsetReset,
                               clientId = clientId
                             )
                           )
                         )
                     }.provideSomeEnvironment[GreyhoundMetrics](_.add(zio.Scope.global))
    } yield new GreyhoundConsumers {
      override def pause: Future[Unit] =
        config.runtime.unsafeRunToFuture(ZIO.foreach(consumers)(_.pause).unit)

      override def resume: Future[Unit] =
        config.runtime.unsafeRunToFuture(ZIO.foreach(consumers)(_.resume).unit)

      override def isAlive: Future[Boolean] =
        config.runtime.unsafeRunToFuture(ZIO.foreach(consumers)(_.isAlive).map(_.forall(_ == true)))

      override def shutdown: Future[Unit] =
        config.runtime.unsafeRunToFuture(ZIO.foreach(consumers)(_.shutdown()).unit)
    }
  }

}
