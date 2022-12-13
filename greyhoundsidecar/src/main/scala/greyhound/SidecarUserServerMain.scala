package greyhound

import scalapb.zio_grpc.{ServerMain, ServiceList, ZTransform}
import zio.ZIO


object SidecarUserServerMain extends ServerMain {

  override def port: Int = Ports.RegisterPort

  override def services: ServiceList[Any] = ServiceList.add(new SidecarUserService)

  override def welcome: ZIO[Any, Throwable, Unit] = super.welcome *> zio.Console.printLine(s"SidecarUserServerMain with port $port")
}
