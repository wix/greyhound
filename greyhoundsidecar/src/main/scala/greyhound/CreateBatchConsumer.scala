package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.consumer.OffsetReset
import com.wixpress.dst.greyhound.core.consumer.batched._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.BatchConsumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.BatchConsumer.RetryStrategy.Blocking
import greyhound.BatchRetryStrategyMapper.asRetryConfig
import zio.ZIO

import scala.concurrent.duration.DurationInt

object CreateBatchConsumer {

  def apply(topic: String, group: String, retryStrategy: RetryStrategy, kafkaAddress: String) =
    for {
      managedClient <- SidecarUserClient.managed
      _             <- ZIO.scoped { // TODO: Fix scoped to be like CreateConsumer + add appropriate test
        managedClient
          .flatMap(client =>
            BatchConsumer.make(
              config = BatchConsumerConfig(
                bootstrapServers = kafkaAddress,
                groupId = group,
                offsetReset = OffsetReset.Earliest,
                initialSubscription = ConsumerSubscription.Topics(Set(topic)),
                retryConfig = asRetryConfig(retryStrategy)
              ),
              handler = BatchConsumerHandler(topic, group, client)
                .withDeserializers(Serdes.StringSerde, Serdes.StringSerde)
            )
          )
          .forever
      }
    } yield ()

}

object BatchRetryStrategyMapper {
  def asRetryConfig(retryStrategy: RetryStrategy): Option[BatchRetryConfig] =
    retryStrategy match {
      case Blocking(value) => Some(BatchRetryConfig.infiniteBlockingRetry(value.interval.millis))
      case _               => None
    }
}
