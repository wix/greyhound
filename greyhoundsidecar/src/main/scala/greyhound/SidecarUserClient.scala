package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.{Scope, ZIO}

object SidecarUserClient {

  def apply(hostDetails: HostDetails): ZIO[Scope, Throwable, GreyhoundSidecarUserClient.ZService[Any]] =
     GreyhoundSidecarUserClient.scoped(channel(hostDetails))

  private def channel(hostDetails: HostDetails): ZManagedChannel=
    ZManagedChannel(
      ManagedChannelBuilder
        .forAddress(hostDetails.host, hostDetails.port)
        .usePlaintext())

}
