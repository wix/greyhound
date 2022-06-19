package greyhound

import com.wixpress.dst.greyhound.testkit.ManagedKafkaConfig
import scalapb.zio_grpc.{ServerMain, ServiceList}
import zio.ZEnv

object SidecarServerMain extends ServerMain {

  override def port: Int = Ports.SidecarGrpcPort

  private val defaultKafkaAddress = s"localhost:${ManagedKafkaConfig.Default.kafkaPort}"

  override def services: ServiceList[ZEnv] = ServiceList.addM {
    for {
      register <- RegisterLive.Default
      kafkaAddress <- EnvArgs.kafkaAddress.map(_.getOrElse(defaultKafkaAddress))
      _ = println(s"~~~ INIT Sidecar Server with kafka address $kafkaAddress")
      _ <- register.updateKafkaAddress(kafkaAddress)
    } yield new SidecarService(register)
  }

}
