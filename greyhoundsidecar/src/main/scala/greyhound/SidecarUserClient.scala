package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel

object SidecarUserClient extends {

  val channel = Register.get.map { host =>
    ZManagedChannel[Any](
      ManagedChannelBuilder
        .forAddress(host.host, host.port)
        .usePlaintext())
  }

  val managed = channel.map(channel => GreyhoundSidecarUserClient.managed(channel))
}
