package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.batched._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.core.consumer.retry.RetryConfig
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.BatchConsumer.RetryStrategy.{Blocking => BatchBlocking}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.BatchConsumer.{RetryStrategy => BatchRetryStrategy}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy.{Blocking, NoRetry, NonBlocking}
import greyhound.ConsumerCreatorImpl.{asBatchRetryConfig, asRetryConfig}
import zio.{Scope, ZIO, ZLayer}

import java.time.Duration
import scala.concurrent.duration.DurationInt
trait ConsumerCreator {
  def createConsumer(hostDetails: TenantHostDetails,
                     topic: String,
                     group: String,
                     retryStrategy: RetryStrategy,
                     kafkaAddress: String,
                     registrationId: String): ZIO[Scope with GreyhoundMetrics, Throwable, Unit]

  def createBatchConsumer(hostDetails: TenantHostDetails,
                          topic: String,
                          group: String,
                          retryStrategy: BatchRetryStrategy,
                          kafkaAddress: String,
                          extraProperties: Map[String, String],
                          registrationId: String): ZIO[GreyhoundMetrics with Scope, Throwable, Unit]
}

class ConsumerCreatorImpl(tenantRegistry: Registry) extends ConsumerCreator {
  override def createConsumer(hostDetails: TenantHostDetails,
                              topic: String,
                              group: String,
                              retryStrategy: RetryStrategy,
                              kafkaAddress: String,
                              registrationId: String
                             ): ZIO[Scope with GreyhoundMetrics, Throwable, Unit] = {
    for {
      client <- SidecarUserClient(hostDetails)
      recordConsumer <- RecordConsumer.make(
        config = RecordConsumerConfig(
          bootstrapServers = kafkaAddress,
          group = group,
          offsetReset = OffsetReset.Earliest,
          initialSubscription = ConsumerSubscription.Topics(Set(topic)),
          retryConfig = asRetryConfig(retryStrategy)
        ),
        handler = ConsumerHandler(topic, group, client)
          .withDeserializers(Serdes.StringSerde, Serdes.StringSerde))
      _ <- tenantRegistry.addConsumer(tenantId = registrationId, topic = topic, consumerGroup = group, shutdown = recordConsumer.shutdown())
    } yield ()
  }

  override def createBatchConsumer(hostDetails: TenantHostDetails,
                                   topic: String,
                                   group: String,
                                   retryStrategy: BatchRetryStrategy,
                                   kafkaAddress: String,
                                   extraProperties: Map[String, String],
                                   registrationId: String
                                  ): ZIO[GreyhoundMetrics with Scope, Throwable, Unit] = {
    for {
      client <- SidecarUserClient(hostDetails)
      batchConsumer <- BatchConsumer.make(
        config = BatchConsumerConfig(
          bootstrapServers = kafkaAddress,
          groupId = group,
          offsetReset = OffsetReset.Earliest,
          initialSubscription = ConsumerSubscription.Topics(Set(topic)),
          retryConfig = asBatchRetryConfig(retryStrategy),
          extraProperties = extraProperties
        ),
        handler = BatchConsumerHandler(topic, group, client)
          .withDeserializers(Serdes.StringSerde, Serdes.StringSerde))
      _ <- tenantRegistry.addConsumer(tenantId = registrationId, topic = topic, consumerGroup = group, shutdown = batchConsumer.shutdown(Duration.ofSeconds(5L)))
    } yield ()
  }
}

object ConsumerCreatorImpl {

  val layer = ZLayer.fromZIO {
    for {
      tenantRegistry <- ZIO.service[Registry]
    } yield new ConsumerCreatorImpl(tenantRegistry = tenantRegistry)
  }
  def asRetryConfig(retryStrategy: RetryStrategy): Option[RetryConfig] =
    retryStrategy match {
      case NoRetry(_)                                     => Some(RetryConfig.empty)
      case Blocking(value)                                => Some(RetryConfig.infiniteBlockingRetry(value.interval.millis))
      // If intervals is empty ignore silently with None, not sure if that is correct
      case NonBlocking(value) if value.intervals.nonEmpty =>
        Some(RetryConfig.nonBlockingRetry(value.intervals.head.millis, value.intervals.tail.map(_.millis): _*))
      case _                                              => None
    }

  def asBatchRetryConfig(retryStrategy: BatchRetryStrategy): Option[BatchRetryConfig] =
    retryStrategy match {
      case BatchBlocking(value) => Some(BatchRetryConfig.infiniteBlockingRetry(value.interval.millis))
      case _ => None
    }
}
