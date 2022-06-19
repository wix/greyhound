package greyhound

import scalapb.zio_grpc.{ServerMain, ServiceList}
import zio.ZEnv

object SidecarServerMain extends ServerMain {

  override def port: Int = Ports.SidecarGrpcPort

  override def services: ServiceList[ZEnv] = ServiceList.addM {
    for {
      register <- RegisterLive.Default
    } yield new SidecarService(register)
  }

}
