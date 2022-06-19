package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.core.consumer.retry.RetryConfig
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import zio.{Chunk, ZIO}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

object CreateConsumer {

  def apply(topic: String, group: String, retryStrategy: RetryStrategy) = {
    val retryConfig = retryStrategy match {
      case RetryStrategy.Empty => throw new Exception("Misconfigured retry strategy")
      case RetryStrategy.NoRetry(_) => None
      case RetryStrategy.Blocking(conf) => Some(RetryConfig.infiniteBlockingRetry(Duration.apply(conf.interval, TimeUnit.MILLISECONDS)))
      case RetryStrategy.NonBlocking(conf) =>
        val Seq(firstInterval, theRest) = conf.intervals.map(i => Duration.apply(i, TimeUnit.MILLISECONDS))
        Some(RetryConfig.nonBlockingRetry(firstInterval, theRest))
    }

    RecordConsumer.make(
      config = RecordConsumerConfig(
        bootstrapServers = Produce.bootstrapServer,
        group = group,
        offsetReset = OffsetReset.Earliest,
        initialSubscription = ConsumerSubscription.Topics(Set(topic)),
        retryConfig = retryConfig),
      handler =
        ConsumerHandler(topic, group)
          .withDeserializers(Serdes.StringSerde, Serdes.StringSerde)
    ).useForever
  }

}
