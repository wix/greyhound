package greyhound

import com.wixpress.dst.greyhound.core.consumer.RecordConsumer
import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.consumer.batched.BatchConsumer
import zio.{Ref, Task, UIO, ZLayer}

trait ConsumerRegistry {
  def add(topic: String,
          consumerGroup: String,
          registrationId: String,
          recordConsumer: Either[BatchConsumer[Any], RecordConsumer[Any with Env]]
         ): Task[Unit]

  def get(topic: String, consumerGroup: String): UIO[Option[ConsumerInfo]]

  def remove(topic: String, consumerGroup: String): Task[Unit]
}

case class ConsumerRegistryLive(ref: Ref[Map[(String, String), ConsumerInfo]]) extends ConsumerRegistry {

  override def add(topic: String,
                   consumerGroup: String,
                   registrationId: String,
                   recordConsumer: Either[BatchConsumer[Any], RecordConsumer[Any with Env]],
                  ): Task[Unit] =
    ref.update(_.updated((topic, consumerGroup), ConsumerInfo(topic, consumerGroup, registrationId, recordConsumer)))

  override def get(topic: String, consumerGroup: String): UIO[Option[ConsumerInfo]] =
    ref.get.map(_.get((topic, consumerGroup)))

  override def remove(topic: String, consumerGroup: String): Task[Unit] =
    ref.update(_ - ((topic, consumerGroup)))
}

object ConsumerRegistryLive {

  val layer = ZLayer {
    Ref.make(Map.empty[(String, String), ConsumerInfo])
      .map(ConsumerRegistryLive(_))
  }

}

case class ConsumerInfo(topic: String,
                        consumerGroup: String,
                        registrationId: String,
                        recordConsumer: Either[BatchConsumer[Any], RecordConsumer[Any with Env]]
                       )
