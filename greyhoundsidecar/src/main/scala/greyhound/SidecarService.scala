package greyhound

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar.RGreyhoundSidecar
import io.grpc.Status
import zio.{ULayer, ZEnv, ZIO, ZLayer}
import zio.console.putStrLn

class SidecarService(register: Register.Service) extends RGreyhoundSidecar[ZEnv] {

  override def register(request: RegisterRequest): ZIO[ZEnv, Status, RegisterResponse] =
    register0(request)
      .mapError(Status.fromThrowable)

  private def register0(request: RegisterRequest) = for {
    port <- ZIO.effect(request.port.toInt)
    _    <- register.add(request.host, port)
    _    <- putStrLn(s"~~~ REGISTER $request ~~~").orDie
  } yield RegisterResponse()

  override def produce(request: ProduceRequest): ZIO[ZEnv, Status, ProduceResponse] =
    produce0(request)
      .mapError(Status.fromThrowable)
      .as(ProduceResponse())

  private def produce0(request: ProduceRequest) =
    for {
      _            <- putStrLn(s"~~~ START PRODUCE $request~~~").orDie
      kafkaAddress <- register.get.map(_.kafkaAddress)
      _            <- Produce(request, kafkaAddress)
    } yield ()

  override def createTopics(request: CreateTopicsRequest): ZIO[ZEnv, Status, CreateTopicsResponse] =
    createTopics0(request)
      .mapError(Status.fromThrowable)
      .as(CreateTopicsResponse())

  private def createTopics0(request: CreateTopicsRequest) =
    for {
      _            <- putStrLn(s"~~~ START CREATE TOPICS $request ~~~").orDie
      kafkaAddress <- register.get.map(_.kafkaAddress)
      _            <- SidecarAdminClient.admin(kafkaAddress).use { client => client.createTopics(request.topics.toSet.map(mapTopic)) }
      _            <- putStrLn("~~~ END CREATE TOPICS ~~~")
    } yield ()

  private def mapTopic(topic: TopicToCreate): TopicConfig =
    TopicConfig(name = topic.name, partitions = topic.partitions.getOrElse(1), replicationFactor = 1, cleanupPolicy = CleanupPolicy.Compact)

  override def startConsuming(request: StartConsumingRequest): ZIO[ZEnv, Status, StartConsumingResponse] =
    startConsuming0(request)
      .provideCustomLayer(ZLayer.succeed(register) ++ DebugMetrics.layer)
      .as(StartConsumingResponse())

  private def startConsuming0(request: StartConsumingRequest) =
    ZIO.foreach(request.consumers) { consumer =>
      println(s"~~~ CREATE CONSUMER $request~~~")
      CreateConsumer(consumer.topic, consumer.group, consumer.retryStrategy).forkDaemon
    }

}
