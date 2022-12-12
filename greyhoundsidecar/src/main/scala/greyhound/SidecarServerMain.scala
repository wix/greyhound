package greyhound

import scalapb.zio_grpc.{ServerMain, ServiceList}
import zio.UIO

class SidecarServerMain(registerService: UIO[Register.Service], kafkaAddress: String) extends ServerMain {

  override def port: Int = Ports.SidecarGrpcPort


  override def services: ServiceList[Any] = ServiceList.addScoped {
    for {
      kafkaAddress <- EnvArgs.kafkaAddress.map(_.getOrElse(kafkaAddress))
      register     <- registerService
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
