package greyhound

import io.grpc.Status
import scalapb.zio_grpc.{RequestContext, ServerMain, ServiceList, ZTransform}
import zio.logging.backend.SLF4J
import zio.stream.ZStream
import zio.{Cause, Ref, Runtime, URIO, ZIO}

object SidecarServerMain extends ServerMain {

  // Configure all logs to use slf4j/logback
  override val bootstrap = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def port: Int = Ports.SidecarGrpcPort

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


  override def services: ServiceList[Any] = ServiceList.addZIO {
    for {
      kafkaAddress <- EnvArgs.kafkaAddress.map(_.getOrElse(throw new RuntimeException("kafka address is not configured")))
      hostDetailsRef <- Ref.make(Map.empty[String, HostDetails])
      consumerRegistryRef <- Ref.make(Map.empty[(String, String), ConsumerInfo])
      consumerRegistry = ConsumerRegistryLive(consumerRegistryRef)
      register = RegisterLive(hostDetailsRef)
      _ = println(
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
      _ = println(s"~~~ INIT Sidecar Server with kafka address ${kafkaAddress}")
    } yield new SidecarService(register, consumerRegistry, kafkaAddress = kafkaAddress)
      .transform[RequestContext](new LoggingTransform)
  }

  override def welcome: ZIO[Any, Throwable, Unit] = super.welcome *> zio.Console.printLine(s"SidecarServerMain with port $port")

}
