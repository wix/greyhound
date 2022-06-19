package greyhound

import scalapb.zio_grpc.{ServerMain, ServiceList}
import zio.ZEnv

class SidecarServerMain(kafkaAddress: String) extends ServerMain {

  override def port: Int = Ports.SidecarGrpcPort

  override def services: ServiceList[ZEnv] = ServiceList.addM {
    for {
      register <- RegisterLive.Default
      _ <- register.updateKafkaAddress(kafkaAddress)
    } yield new SidecarService(register)
  }

}
