package greyhound.support

import greyhound.{ConsumerInfo, ConsumerRegistryLive, HostDetails, RegisterLive, SidecarService}
import zio.{Ref, ULayer, ZLayer}

trait SidecarTestSupport {
  val testContextLayer: ULayer[TestContext] = ZLayer.succeed(TestContext.random)

  def sidecarServiceLayer(kafkaAddress: String) = ZLayer.fromZIO {
    for {
      registerRef <- Ref.make(Map.empty[String, HostDetails])
      register = RegisterLive(registerRef)
      consumerRegistryRef <- Ref.make(Map.empty[(String, String), ConsumerInfo])
      consumerRegistry = ConsumerRegistryLive(consumerRegistryRef)
    } yield new SidecarService(register, consumerRegistry, kafkaAddress = kafkaAddress)
  }
}
