package greyhound

import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar.RGreyhoundSidecar
import io.grpc.Status
import zio.{ZEnv, ZIO}
import zio.console.putStrLn

object SidecarService extends RGreyhoundSidecar[ZEnv] {

  override def register(request: RegisterRequest): ZIO[ZEnv, Status, RegisterResponse] =
    register0(request)
      .provideCustomLayer(RegisterLive.Layer)
      .mapError(Status.fromThrowable)

    private def register0(request: RegisterRequest) = for {
      port <- ZIO.effect(request.port.toInt)
      _ <- Register.add(request.host, port)
      _ <- putStrLn("~~~ REGISTER ~~~").orDie
    } yield RegisterResponse()

  override def produce(request: ProduceRequest): ZIO[ZEnv, Status, ProduceResponse] =
    produce0(request)
      .mapError(Status.fromThrowable)
      .map(_ => ProduceResponse())

  private def produce0(request: ProduceRequest) =
    putStrLn("~~~ START PRODUCE ~~~").orDie *>
      Produce(request)
        .tap(response => putStrLn(s"~~~ REACHED SERVER PRODUCE. response: $response"))

  override def createTopics(request: CreateTopicsRequest): ZIO[ZEnv, Status, CreateTopicsResponse] =
    createTopics0(request)
      .mapError(Status.fromThrowable)
      .map(_ => CreateTopicsResponse())

  private def createTopics0(request: CreateTopicsRequest) =
    putStrLn("~~~ START CREATE TOPICS ~~~").orDie *>
      SidecarAdminClient.admin.use { client =>
        client.createTopics(request.topics.toSet.map(mapTopic))
      } *>
      putStrLn("~~~ END CREATE TOPICS ~~~")

  private def mapTopic(topic: TopicToCreate): TopicConfig =
    TopicConfig(
      name = topic.name,
      partitions = topic.partitions.getOrElse(1),
      replicationFactor = 1,
      cleanupPolicy = CleanupPolicy.Compact)

  override def startConsuming(request: StartConsumingRequest): ZIO[ZEnv, Status, StartConsumingResponse] =
    ???
}
