package greyhound

import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ProducerRecord}
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar.RGreyhoundSidecar
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import io.grpc.Status
import zio.{IO, Scope, UIO, ZIO, ZLayer}

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
    _ <- register.add(request.host, port)
  } yield RegisterResponse()

  override def produce(request: ProduceRequest): IO[Status, ProduceResponse] =
    ZIO.scoped {
      produce0(request)
        .mapError(Status.fromThrowable)
        .as(ProduceResponse())
    }

  private def produce0(request: ProduceRequest) =
    for {
      producer <- producer
      _ <- Produce(request, producer).tapError(e => zio.Console.printLine(e))
    } yield ()

  override def createTopics(request: CreateTopicsRequest): IO[Status, CreateTopicsResponse] =
    createTopics0(request)
      .mapError(Status.fromThrowable)
      .as(CreateTopicsResponse())


  private def createTopics0(request: CreateTopicsRequest) = ZIO.scoped {
    for {
      client <- SidecarAdminClient.admin(kafkaAddress)
      _ <- client.createTopics(request.topics.toSet.map(mapTopic)).provideSomeLayer(DebugMetrics.layer)
    } yield ()
  }

  private def mapTopic(topic: TopicToCreate): TopicConfig =
    TopicConfig(name = topic.name, partitions = topic.partitions.getOrElse(1), replicationFactor = 1, cleanupPolicy = CleanupPolicy.Compact)

  override def startConsuming(request: StartConsumingRequest): IO[Status with Scope, StartConsumingResponse] = {
    startConsuming0(request)
      .provideSomeLayer(ZLayer.succeed(register) ++ DebugMetrics.layer)
      .as(StartConsumingResponse())
  }

  private def startConsuming0(request: StartConsumingRequest) =
    ZIO.foreach(request.consumers) { consumer =>
      CreateConsumer(consumer.topic, consumer.group, consumer.retryStrategy, kafkaAddress).forkDaemon
    } *>
      ZIO.foreach(request.batchConsumers) { consumer =>
        CreateBatchConsumer(consumer.topic, consumer.group, consumer.retryStrategy, kafkaAddress, consumer.extraProperties).forkDaemon
      }

}


