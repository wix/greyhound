package greyhound

import scalapb.zio_grpc.{ServerMain, ServiceList}

object SidecarUserServerMain extends ServerMain {

  override def port: Int = Ports.RegisterPort

  override def services: ServiceList[Any] = ServiceList.add(new SidecarUserService)
}
