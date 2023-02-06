package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.core.consumer.retry.RetryConfig
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy.{Blocking, NoRetry, NonBlocking}
import greyhound.RetryStrategyMapper.asRetryConfig
import zio.{Scope, ZIO}

import scala.concurrent.duration.DurationInt

object CreateConsumer {

  def apply(hostDetails: TenantHostDetails,
            topic: String,
            group: String,
            retryStrategy: RetryStrategy,
            kafkaAddress: String): ZIO[Scope with GreyhoundMetrics, Throwable, Unit] =
    for {
      client <- SidecarUserClient(hostDetails)
      _ <- RecordConsumer.make(
        config = RecordConsumerConfig(
          bootstrapServers = kafkaAddress,
          group = group,
          offsetReset = OffsetReset.Earliest,
          initialSubscription = ConsumerSubscription.Topics(Set(topic)),
          retryConfig = asRetryConfig(retryStrategy)
        ),
        handler = ConsumerHandler(topic, group, client)
          .withDeserializers(Serdes.StringSerde, Serdes.StringSerde))
    } yield ()

}

object RetryStrategyMapper {
  def asRetryConfig(retryStrategy: RetryStrategy): Option[RetryConfig] =
    retryStrategy match {
      case NoRetry(_)                                     => Some(RetryConfig.empty)
      case Blocking(value)                                => Some(RetryConfig.infiniteBlockingRetry(value.interval.millis))
      // If intervals is empty ignore silently with None, not sure if that is correct
      case NonBlocking(value) if value.intervals.nonEmpty =>
        Some(RetryConfig.nonBlockingRetry(value.intervals.head.millis, value.intervals.tail.map(_.millis): _*))
      case _                                              => None
    }
}
