package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar
import io.grpc.Status
import scalapb.zio_grpc.{RequestContext, ServerMain, ServiceList, ZTransform}
import zio.logging.backend.SLF4J
import zio.redis._
import zio.stream.ZStream
import zio.{Cause, Runtime, UIO, URIO, ZIO, ZLayer}

object SidecarServerMain extends ServerMain {

  // Configure all logs to use slf4j/logback
  override val bootstrap = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def port: Int = Ports.SidecarGrpcPort

  override def services: ServiceList[Any] = ServiceList.addZIO(initSidecarService)

  override def welcome: ZIO[Any, Throwable, Unit] =
    super.welcome *>
      zio.Console.printLine(s"SidecarServerMain with port $port") *>
      zio.Console.printLine(
        """   ____                _                           _
          |  / ___|_ __ ___ _   _| |__   ___  _   _ _ __   __| |
          | | |  _| '__/ _ \ | | | '_ \ / _ \| | | | '_ \ / _` |
          | | |_| | | |  __/ |_| | | | | (_) | |_| | | | | (_| |
          |  \____|_|  \___|\__, |_| |_|\___/ \__,_|_| |_|\__,_|
          | / ___|(_) __| | |___/___ __ _ _ __
          | \___ \| |/ _` |/ _ \/ __/ _` | '__|
          |  ___) | | (_| |  __/ (_| (_| | |
          | |____/|_|\__,_|\___|\___\__,_|_|
          |""".stripMargin)

  class LoggingTransform[R] extends ZTransform[R, Status, R with RequestContext] {

    def logCause(cause: Cause[Status]): URIO[RequestContext, Unit] = ZIO.logCause(cause)

    def accessLog: URIO[RequestContext, Unit] = for {
      context <- ZIO.service[RequestContext]
      _ <- zio.Console.printLine(s"Request method: ${context.methodDescriptor.getBareMethodName}, attributes: ${context.attributes}").orDie
    } yield ()

    override def effect[A](io: ZIO[R, Status, A]): ZIO[R with RequestContext, Status, A] =
      io.zipLeft(accessLog).tapErrorCause(logCause)

    override def stream[A](io: ZStream[R, Status, A]): ZStream[R with RequestContext, Status, A] = (io ++ ZStream.fromZIO(accessLog).drain)
      .onError(logCause)
  }

  private def getRedisConfig: UIO[RedisConfig] = {
    val maybeConfig = for {
      redisHost <- ZIO.fromOption(scala.util.Properties.envOrNone("REDIS_HOST"))
      redisPortEnv <- ZIO.fromOption(scala.util.Properties.envOrNone("REDIS_PORT"))
      redisPort <- ZIO.attempt(redisPortEnv.toInt)
    } yield RedisConfig(redisHost, redisPort)

    maybeConfig.catchAll(_ => ZIO.succeed(RedisConfig.Default))
  }

  private val redisConfigLayer = ZLayer.fromZIO(getRedisConfig)

  // Currently supports only single node
  private lazy val redisLayer: ZLayer[Any, RedisError.IOError, Redis] = ZLayer.make[Redis](
    Redis.layer,
    RedisExecutor.layer,
    redisConfigLayer,
    ZLayer.succeed(CodecSupplier.utf8string),
  )

  private lazy val redisRegistryRepo = ZLayer.make[RedisRegistryRepo](
    ZLayer.fromFunction(RedisRegistryRepo(_)),
    redisLayer,
  )

  private val sidecarService = for {
    service <- ZIO.service[SidecarService]
    kafkaAddress <- ZIO.service[KafkaInfo].map(_.address)
    _ <- zio.Console.printLine(s"~~~ INIT Sidecar Service with kafka address $kafkaAddress").orDie
  } yield service.transform[RequestContext](new LoggingTransform)

  private def getStandaloneMode: UIO[Boolean] = ZIO.attempt(
    scala.util.Properties.envOrNone("STANDALONE_MODE").exists(_.toBoolean)
  ).catchAll(_ => ZIO.succeed(false))

  private def initSidecarService: ZIO[Any, Throwable, ZioGreyhoundsidecar.ZGreyhoundSidecar[RequestContext]] = for {
    isStandalone <- getStandaloneMode
    partialService = sidecarService.provideSome[RegistryRepo](
      SidecarService.layer,
      TenantRegistry.layer,
      KafkaInfoLive.layer,
      ConsumerCreatorImpl.layer,
    )
    service <- if(isStandalone) partialService.provide(redisRegistryRepo) else partialService.provide(InMemoryRegistryRepo.layer)
  } yield service
}

class LoggingTransform[R] extends ZTransform[R, Status, R with RequestContext] {

  def logCause(cause: Cause[Status]): URIO[RequestContext, Unit] = ZIO.logCause(cause)

  def accessLog: URIO[RequestContext, Unit] = for {
    context <- ZIO.service[RequestContext]
    _ <- zio.Console.printLine(s"Request method: ${context.methodDescriptor.getBareMethodName}, attributes: ${context.attributes}").orDie
  } yield ()

  override def effect[A](io: ZIO[R, Status, A]): ZIO[R with RequestContext, Status, A] =
    io.zipLeft(accessLog).tapErrorCause(logCause)

  override def stream[A](io: ZStream[R, Status, A]): ZStream[R with RequestContext, Status, A] = (io ++ ZStream.fromZIO(accessLog).drain)
    .onError(logCause)
}
