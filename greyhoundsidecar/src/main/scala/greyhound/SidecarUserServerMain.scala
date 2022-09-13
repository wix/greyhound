package greyhound

import scalapb.zio_grpc.{ServerMain, ServiceList}
import zio.{ZEnv, ZIO}

object SidecarUserServerMain extends ServerMain {

  override def port: Int = Ports.RegisterPort

  override def services: ServiceList[zio.ZEnv] = ServiceList.addM {
    ZIO(new SidecarUserService)
  }
}
