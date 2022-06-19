package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ZioGreyhoundsidecar.GreyhoundSidecarClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel

object SidecarClient {

  val channel = ZManagedChannel[Any](
    ManagedChannelBuilder
      .forAddress("localhost", Ports.SidecarGrpcPort)
      .usePlaintext()
  )

  val managed = GreyhoundSidecarClient.managed(channel)
}
