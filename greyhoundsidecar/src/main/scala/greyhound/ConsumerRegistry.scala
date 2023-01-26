package greyhound

import zio.{FiberId, Ref, Task, UIO, ZLayer}

trait ConsumerRegistry {
  def add(topic: String, consumerGroup: String, registrationId: Option[String], fiberId: Option[FiberId.Runtime]): Task[Unit]
  def get(topic: String, consumerGroup: String): UIO[Option[ConsumerInfo]]
}

case class ConsumerRegistryLive(ref: Ref[Map[(String, String), ConsumerInfo]]) extends ConsumerRegistry {

  override def add(topic: String, consumerGroup: String, registrationId: Option[String], fiberId: Option[FiberId.Runtime]): Task[Unit] =
    ref.update(_.updated((topic, consumerGroup), ConsumerInfo(topic, consumerGroup, registrationId, fiberId)))

  override def get(topic: String, consumerGroup: String): UIO[Option[ConsumerInfo]] =
    ref.get.map(_.get((topic, consumerGroup)))
}

object ConsumerRegistryLive {

  val layer = ZLayer {
    Ref.make(Map.empty[(String, String), ConsumerInfo])
      .map(ConsumerRegistryLive(_))
  }

}

case class ConsumerInfo(topic: String,
                        consumerGroup: String,
                        registrationId: Option[String] = None,
                        fiberId: Option[FiberId.Runtime] = None)
