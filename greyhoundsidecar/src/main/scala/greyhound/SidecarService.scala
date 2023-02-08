package greyhound

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord}
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar.RCGreyhoundSidecar
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import io.grpc.Status
import zio.{Fiber, IO, Scope, UIO, ZIO, ZLayer}

import java.util.UUID

class SidecarService(tenantRegistry: Registry,
                     consumerCreator: ConsumerCreator,
                     onProduceListener: ProducerRecord[_, _] => UIO[Unit] = _ => ZIO.unit,
                     kafkaAddress: String
                    ) extends RCGreyhoundSidecar {

  private val producer = Producer.make(ProducerConfig(kafkaAddress, onProduceListener = onProduceListener))

  override def register(request: RegisterRequest): IO[Status, RegisterResponse] =
    register0(request)
      .mapError(Status.fromThrowable)

  private def register0(request: RegisterRequest) = for {
    port <- ZIO.attempt(request.port.toInt)
    tenantId = UUID.randomUUID().toString
    _ <- tenantRegistry.addTenant(tenantId = tenantId, host = request.host, port = port)
  } yield RegisterResponse(registrationId = tenantId)

  override def produce(request: ProduceRequest): IO[Status, ProduceResponse] =
    ZIO.scoped {
      produce0(request)
        .mapBoth(Status.fromThrowable, _ => ProduceResponse())
    }

  private def produce0(request: ProduceRequest) =
    for {
      producer <- producer
      _ <- Produce(request, producer).tapError(e => zio.Console.printLine(e))
    } yield ()

  override def createTopics(request: CreateTopicsRequest): IO[Status, CreateTopicsResponse] =
    createTopics0(request)
      .mapBoth(Status.fromThrowable, _ => CreateTopicsResponse())


  private def createTopics0(request: CreateTopicsRequest) = ZIO.scoped {
    for {
      client <- SidecarAdminClient.admin(kafkaAddress)
      _ <- client.createTopics(request.topics.toSet.map(mapTopic)).provideSomeLayer(DebugMetrics.layer)
    } yield ()
  }

  private def mapTopic(topic: TopicToCreate): TopicConfig =
    TopicConfig(name = topic.name, partitions = topic.partitions.getOrElse(1), replicationFactor = 1, cleanupPolicy = CleanupPolicy.Compact)

  override def startConsuming(request: StartConsumingRequest): IO[Status, StartConsumingResponse] =
    for {
      _ <- validateStartConsumingRequest(request)
      _ <- tenantRegistry.getTenant(tenantId = request.registrationId).flatMap {
        case Some(tenantInfo) =>
          startConsuming0(tenantInfo.hostDetails, request.consumers, request.batchConsumers, request.registrationId)
            .mapError(Status.fromThrowable)
            .provideSomeLayer(DebugMetrics.layer ++ ZLayer.succeed(Scope.global))

        case None => ZIO.fail(Status.NOT_FOUND.withDescription(s"registrationId ${request.registrationId} not found"))
      }
    } yield StartConsumingResponse()

  private def validateStartConsumingRequest(request: StartConsumingRequest): ZIO[Any, Status, Unit] = {
    val candidateConsumers = request.consumers.map(consumer => TenantConsumerInfo(consumer.topic, consumer.group, shutdown = ZIO.unit)) ++
      request.batchConsumers.map(consumer => TenantConsumerInfo(consumer.topic, consumer.group, shutdown = ZIO.unit))
    if (candidateConsumers.size == candidateConsumers.toSet.size) {
      ZIO.forall(candidateConsumers) { candidateConsumer =>
        tenantRegistry.isUniqueConsumer(candidateConsumer.topic, candidateConsumer.consumerGroup, request.registrationId)
      }.flatMap { isUnique => if (isUnique) ZIO.succeed() else ZIO.fail(Status.ALREADY_EXISTS) }
    } else
      ZIO.fail(Status.INVALID_ARGUMENT
        .withDescription(s"Creating multiple consumers for the same topic + group is not allowed"))
  }

  private def startConsuming0(hostDetails: TenantHostDetails,
                              consumers: Seq[Consumer],
                              batchConsumers: Seq[BatchConsumer],
                              registrationId: String): ZIO[GreyhoundMetrics with Scope, Throwable, Seq[Fiber.Runtime[Throwable, Unit]]] =
    ZIO.foreachPar(consumers) { consumer =>
      for {
        fiber <- consumerCreator.createConsumer(
          hostDetails = hostDetails,
          topic = consumer.topic,
          group = consumer.group,
          retryStrategy = consumer.retryStrategy,
          kafkaAddress = kafkaAddress,
          registrationId = registrationId
        ).forkDaemon
      } yield fiber
    } &>
      ZIO.foreachPar(batchConsumers) { consumer =>
        for {
          fiber <- consumerCreator.createBatchConsumer(
            hostDetails = hostDetails,
            topic = consumer.topic,
            group = consumer.group,
            retryStrategy = consumer.retryStrategy,
            kafkaAddress = kafkaAddress,
            extraProperties = consumer.extraProperties,
            registrationId = registrationId
          ).forkDaemon
        } yield fiber
      }

  override def stopConsuming(request: StopConsumingRequest): IO[Status, StopConsumingResponse] = {
    for {
      _ <- validateConsumersExist(request.consumersDetails, request.registrationId)
      _ <- stopConsuming0(request)
    } yield StopConsumingResponse()
  }

  private def stopConsuming0(request: StopConsumingRequest): ZIO[Any, Status, Seq[Unit]] = {
    ZIO.foreachPar(request.consumersDetails) { consumerDetails =>
      for {
        maybeConsumerInfo <- tenantRegistry.getConsumer(tenantId = request.registrationId, topic = consumerDetails.topic, consumerGroup = consumerDetails.group)
        _ <- maybeConsumerInfo match {
          case Some(consumerInfo) =>
            for {
              _ <- consumerInfo.shutdown
                .mapError(Status.fromThrowable)
                .provideSomeLayer(DebugMetrics.layer ++ ZLayer.succeed(Scope.global))
              result <- tenantRegistry.removeConsumer(tenantId = request.registrationId, topic = consumerInfo.topic, consumerGroup = consumerInfo.consumerGroup).mapError(Status.fromThrowable)
            } yield result
          case None =>
            ZIO.fail(Status.NOT_FOUND)
        }
      } yield ()
    }
  }

  private def validateConsumersExist(consumers: Seq[ConsumerDetails], registrationId: String): ZIO[Any, Status, Set[Unit]] =
    ZIO.foreachPar(consumers.toSet) { consumer =>
      tenantRegistry.getConsumer(tenantId = registrationId, topic = consumer.topic, consumerGroup = consumer.group).flatMap {
        case Some(_) => ZIO.succeed(())
        case _ => ZIO.fail(Status.NOT_FOUND)
      }
    }
}

object SidecarService {
  val layer: ZLayer[Registry with ConsumerCreator with KafkaInfo, Nothing, SidecarService] = ZLayer.fromZIO {
    for {
      kafkaAddress <- ZIO.service[KafkaInfo].map(_.address)
      tenantRegistry <- ZIO.service[Registry]
      consumerCreator <- ZIO.service[ConsumerCreator]
    } yield new SidecarService(tenantRegistry = tenantRegistry, consumerCreator = consumerCreator, kafkaAddress = kafkaAddress)
  }
}


