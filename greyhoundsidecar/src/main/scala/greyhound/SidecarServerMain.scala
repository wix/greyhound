package greyhound

import io.grpc.Status
import scalapb.zio_grpc.{RequestContext, ServerMain, ServiceList, ZTransform}
import zio.logging.backend.SLF4J
import zio.stream.ZStream
import zio.{Cause, Ref, Runtime, URIO, ZIO}

object SidecarServerMain extends ServerMain {

  class LoggingTransform[R] extends ZTransform[R, Status, R with RequestContext] {

    def logCause(cause: Cause[Status]): URIO[RequestContext, Unit] = ???

    def accessLog: URIO[RequestContext, Unit] = ???

    override def effect[A](io: ZIO[R, Status, A]): ZIO[R with RequestContext, Status, A] =
      io.zipLeft(accessLog).tapErrorCause(logCause)

    override def stream[A](io: ZStream[R, Status, A]): ZStream[R with RequestContext, Status, A] = (io ++ ZStream.fromZIO(accessLog).drain)
      .onError(logCause)
  }

  // Configure all logs to use slf4j/logback
  override val bootstrap = Runtime.removeDefaultLoggers >>> SLF4J.slf4j

  override def port: Int = Ports.SidecarGrpcPort

  override def services: ServiceList[Any] = ServiceList.addScoped {
    for {
      kafkaAddress <- EnvArgs.kafkaAddress.map(_.getOrElse(throw new RuntimeException("kafka address is not configured")))
      dbRef        <- Ref.make(Database(HostDetails("localhost", Ports.RegisterPort), ""))
      register      = RegisterLive(dbRef)
      _            <- register.updateKafkaAddress(kafkaAddress)
      db           <- register.get
      _             = println("""   ____                _                           _
                    |  / ___|_ __ ___ _   _| |__   ___  _   _ _ __   __| |
                    | | |  _| '__/ _ \ | | | '_ \ / _ \| | | | '_ \ / _` |
                    | | |_| | | |  __/ |_| | | | | (_) | |_| | | | | (_| |
                    |  \____|_|  \___|\__, |_| |_|\___/ \__,_|_| |_|\__,_|
                    | / ___|(_) __| | |___/___ __ _ _ __
                    | \___ \| |/ _` |/ _ \/ __/ _` | '__|
                    |  ___) | | (_| |  __/ (_| (_| | |
                    | |____/|_|\__,_|\___|\___\__,_|_|
                    |""".stripMargin)
      _             = println(s"~~~ INIT Sidecar Server with kafka address ${db.kafkaAddress}")
    } yield new SidecarService(register).transform(new LoggingTransform[Any])
  }

  override def welcome: ZIO[Any, Throwable, Unit] = super.welcome *> zio.Console.printLine(s"SidecarServerMain with port $port")

}
