package support

import greyhound.HostDetails
import greyhound.Register.Register
import zio.{Task, UIO, ULayer, ZIO, ZLayer}

trait SidecarTestSupport {

  val localhost = "localhost"

  val kafkaAddress = "localhost:6667"

  val sideCarUserGrpcPort = 9100


  val testContextLayer: ULayer[TestContext] = ZLayer.succeed(TestContext.random)

  object DefaultRegister extends Register {

    private var hostDetails = HostDetails("", 0)

    override def add(host: String, port: Int): Task[Unit] = {
      hostDetails = hostDetails.copy(host, port)
      ZIO.unit
    }

    override val get: UIO[HostDetails] = ZIO.succeed(hostDetails)
  }
}
