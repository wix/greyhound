package greyhound

import scalapb.zio_grpc.{ServerMain, ServiceList}
import zio.logging.backend.SLF4J
import zio.{Ref, Runtime, ZIO}

object SidecarServerMain extends ServerMain {

  // Configure all logs to use slf4j/logback
  override val bootstrap = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def port: Int = Ports.SidecarGrpcPort


  override def services: ServiceList[Any] = ServiceList.addScoped {
    for {
      kafkaAddress <- EnvArgs.kafkaAddress.map(_.getOrElse(throw new RuntimeException("kafka address is not configured")))
      hostDetailsRef <- Ref.make(HostDetails("localhost", Ports.RegisterPort))
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
    } yield new SidecarService(register, kafkaAddress = kafkaAddress)
  }

  override def welcome: ZIO[Any, Throwable, Unit] = super.welcome *> zio.Console.printLine(s"SidecarServerMain with port $port")

}
