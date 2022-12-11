package greyhound

import com.wixpress.dst.greyhound.testkit.ManagedKafkaConfig
import scalapb.zio_grpc.{ServerMain, ServiceList}

object SidecarServerMain extends ServerMain {

  override def port: Int = Ports.SidecarGrpcPort

  private val defaultKafkaAddress = s"localhost:${ManagedKafkaConfig.Default.kafkaPort}"

  override def services: ServiceList[Any] = ServiceList.addScoped {
    for {
      register     <- RegisterLive.Default
      kafkaAddress <- EnvArgs.kafkaAddress.map(_.getOrElse(defaultKafkaAddress))
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
    } yield new SidecarService(register)
  }

}
