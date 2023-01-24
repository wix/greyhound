package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.consumer.OffsetReset
import com.wixpress.dst.greyhound.core.consumer.batched._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.BatchConsumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.BatchConsumer.RetryStrategy.Blocking
import greyhound.BatchRetryStrategyMapper.asRetryConfig
import greyhound.Register.Register
import zio.{Scope, ZLayer}

import scala.concurrent.duration.DurationInt

object CreateBatchConsumer {

  def apply(tenantId: String, topic: String, group: String, retryStrategy: RetryStrategy,
            kafkaAddress: String, extraProperties: Map[String, String]) =
    for {
      managedClient <- SidecarUserClient.managed(tenantId)
      _ <- managedClient.flatMap(client =>
        BatchConsumer.make(
          config = BatchConsumerConfig(
            bootstrapServers = kafkaAddress,
            groupId = group,
            offsetReset = OffsetReset.Earliest,
            initialSubscription = ConsumerSubscription.Topics(Set(topic)),
            retryConfig = asRetryConfig(retryStrategy),
            extraProperties = extraProperties
          ),
          handler = BatchConsumerHandler(topic, group, client)
            .withDeserializers(Serdes.StringSerde, Serdes.StringSerde)
        )
      ).provideSomeLayer[GreyhoundMetrics with Register](ZLayer.succeed(Scope.global))
    } yield ()

}

object BatchRetryStrategyMapper {
  def asRetryConfig(retryStrategy: RetryStrategy): Option[BatchRetryConfig] =
    retryStrategy match {
      case Blocking(value) => Some(BatchRetryConfig.infiniteBlockingRetry(value.interval.millis))
      case _ => None
    }
}
