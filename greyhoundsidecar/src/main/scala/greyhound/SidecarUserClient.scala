package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.{Scope, ZIO}

object SidecarUserClient {

  def apply(hostDetails: TenantHostDetails): ZIO[Scope, Throwable, GreyhoundSidecarUserClient.ZService[Any]] =
     GreyhoundSidecarUserClient.scoped(channel(hostDetails))

  private def channel(hostDetails: TenantHostDetails): ZManagedChannel=
    ZManagedChannel(
      ManagedChannelBuilder
        .forAddress(hostDetails.host, hostDetails.port)
        .usePlaintext())

}
