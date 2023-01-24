package greyhound

import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord}
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar.RGreyhoundSidecar
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import io.grpc.Status
import zio.{IO, Scope, UIO, ZIO, ZLayer}
import java.util.UUID

class SidecarService(register: Register.Service,
                     onProduceListener: ProducerRecord[_, _] => UIO[Unit] = _ => ZIO.unit,
                     kafkaAddress: String
                    ) extends RGreyhoundSidecar[Any] {

  private val producer = Producer.make(ProducerConfig(kafkaAddress, onProduceListener = onProduceListener))

  override def register(request: RegisterRequest): IO[Status, RegisterResponse] =
    register0(request)
      .mapError(Status.fromThrowable)

  private def register0(request: RegisterRequest) = for {
    port <- ZIO.attempt(request.port.toInt)
    tenantId = UUID.randomUUID().toString
    _ <- register.add(tenantId, request.host, port)
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
      _ <- register.get(request.registrationId).flatMap {
        case Some(hostDetails) =>
          startConsuming0(hostDetails, request.consumers, request.batchConsumers)
            .provideSomeLayer(DebugMetrics.layer ++ ZLayer.succeed(Scope.global))

        case None => ZIO.fail(Status.NOT_FOUND.withDescription(s"registrationId ${request.registrationId} not found"))
      }
    } yield StartConsumingResponse()

  private def startConsuming0(hostDetails: HostDetails,
                              consumers: Seq[Consumer],
                              batchConsumers: Seq[BatchConsumer]) =
    ZIO.foreachPar(consumers) { consumer =>
      CreateConsumer(
        hostDetails = hostDetails,
        topic = consumer.topic,
        group = consumer.group,
        retryStrategy = consumer.retryStrategy,
        kafkaAddress = kafkaAddress
      ).forkDaemon
    } &>
      ZIO.foreachPar(batchConsumers) { consumer =>
        CreateBatchConsumer(
          hostDetails = hostDetails,
          topic = consumer.topic,
          group = consumer.group,
          retryStrategy = consumer.retryStrategy,
          kafkaAddress = kafkaAddress,
          extraProperties = consumer.extraProperties
        ).forkDaemon
      }

}


