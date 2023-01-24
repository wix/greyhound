package greyhound.support

import greyhound.{HostDetails, RegisterLive, SidecarService}
import zio.{Ref, ULayer, ZLayer}

trait SidecarTestSupport {
  val testContextLayer: ULayer[TestContext] = ZLayer.succeed(TestContext.random)

  def sidecarServiceLayer(kafkaAddress: String) = ZLayer.fromZIO {
    for {
      ref <- Ref.make(Map.empty[String, HostDetails])
      register = RegisterLive(ref)
    } yield new SidecarService(register, kafkaAddress = kafkaAddress)
  }
}
