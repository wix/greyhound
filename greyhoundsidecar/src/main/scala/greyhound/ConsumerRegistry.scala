package greyhound

import zio.{FiberId, RIO, Ref, Task, UIO, URIO, ZIO}

object ConsumerRegistry {

  trait Service {
    def add(topic: String, consumerGroup: String, registrationId: Option[String], fiberId: Option[FiberId.Runtime]): Task[Unit]
    def get(topic: String, consumerGroup: String): UIO[Option[ConsumerInfo]]
  }

  type ConsumerRegistry = ConsumerRegistry.Service

  def add(topic: String, consumerGroup: String, registrationId: Option[String], fiberId: Option[FiberId.Runtime]): RIO[ConsumerRegistry, Unit] =
    ZIO.serviceWithZIO[ConsumerRegistry.Service](_.add(topic, consumerGroup, registrationId, fiberId))

  def get(topic: String, consumerGroup: String): URIO[ConsumerRegistry, Option[ConsumerInfo]] =
    ZIO.serviceWithZIO[ConsumerRegistry.Service](_.get(topic, consumerGroup))
}

case class ConsumerRegistryLive(ref: Ref[Map[(String, String), ConsumerInfo]]) extends ConsumerRegistry.Service {

  override def add(topic: String, consumerGroup: String, registrationId: Option[String], fiberId: Option[FiberId.Runtime]): Task[Unit] =
    ref.update(_.updated((topic, consumerGroup), ConsumerInfo(topic, consumerGroup, registrationId, fiberId)))

  override def get(topic: String, consumerGroup: String): UIO[Option[ConsumerInfo]] =
    ref.get.map(_.get((topic, consumerGroup)))
}

case class ConsumerInfo(topic: String,
                        consumerGroup: String,
                        registrationId: Option[String] = None,
                        fiberId: Option[FiberId.Runtime] = None)
