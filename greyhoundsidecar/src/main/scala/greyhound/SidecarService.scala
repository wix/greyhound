package greyhound

import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar.RGreyhoundSidecar
import greyhound.Register.Register
import io.grpc.Status
import zio.CanFail.canFailAmbiguous2
import zio.{Fiber, IO, Scope, UIO, ULayer, ZEnvironment, ZIO, ZLayer}

class SidecarService(register: Register.Service) extends RGreyhoundSidecar[Any] {

  override def register(request: RegisterRequest): IO[Status, RegisterResponse] =
    register0(request)
      .mapError(Status.fromThrowable)

  private def register0(request: RegisterRequest) = for {
    port <- ZIO.attempt(request.port.toInt)
    _ <- register.add(request.host, port)
  } yield RegisterResponse()

  override def produce(request: ProduceRequest): IO[Status, ProduceResponse] =
    produce0(request)
      .mapError(Status.fromThrowable)
      .as(ProduceResponse())

  private def produce0(request: ProduceRequest): UIO[Unit] =
    for {
      kafkaAddress <- register.get.map(_.kafkaAddress)
      _ = Produce(request, kafkaAddress)
    } yield ()

  override def createTopics(request: CreateTopicsRequest): IO[Status, CreateTopicsResponse] =
    createTopics0(request)
      .mapError(Status.fromThrowable)
      .as(CreateTopicsResponse())

  private def createTopics0(request: CreateTopicsRequest) = {

    ZIO.scoped {
      for {
        kafkaAddress <- register.get.map(_.kafkaAddress)
        client <- SidecarAdminClient.admin(kafkaAddress)
        _ = client.createTopics(request.topics.toSet.map(mapTopic))
      } yield ()
    }
  }

  private def mapTopic(topic: TopicToCreate): TopicConfig =
    TopicConfig(name = topic.name, partitions = topic.partitions.getOrElse(1), replicationFactor = 1, cleanupPolicy = CleanupPolicy.Compact)

  override def startConsuming(request: StartConsumingRequest): IO[Status with Scope, StartConsumingResponse] = {
    println(s"AMIR inside startConsuming SidecarService")
      startConsuming0(request)
        .provideSomeLayer(ZLayer.succeed(register) ++ DebugMetrics.layer)
        .as(StartConsumingResponse())
  }

  private def startConsuming0(request: StartConsumingRequest) =
    ZIO.foreach(request.consumers) { consumer =>
      println(s"AMIR starting to consume $consumer")
      CreateConsumer(consumer.topic, consumer.group, consumer.retryStrategy).forkDaemon
    } *>
      ZIO.foreach(request.batchConsumers) { consumer =>
        CreateBatchConsumer(consumer.topic, consumer.group, consumer.retryStrategy).forkDaemon
      }

}
