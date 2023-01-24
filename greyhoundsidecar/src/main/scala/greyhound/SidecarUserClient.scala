package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import greyhound.Register.Register
import io.grpc.{ManagedChannelBuilder, Status}
import scalapb.zio_grpc.ZManagedChannel
import zio.{Scope, ZIO}

object SidecarUserClient {


  def channel(tenantId: String): ZIO[Register, Status, ZManagedChannel[Any]] = Register.get(tenantId)
    .flatMap {
      case Some(hostDetails) =>
        // this val construction in needed for IntelliJ to understand the type - god knows why???
        val managedChannel: ZManagedChannel[Any] = ZManagedChannel[Any](
          ManagedChannelBuilder
            .forAddress(hostDetails.host, hostDetails.port)
            .usePlaintext()
        )

        ZIO.succeed(managedChannel)

      case None => ZIO.fail(Status.NOT_FOUND.withDescription(s"Registration id [$tenantId] not found."))
    }

  def managed(tenantId: String): ZIO[Register, Status, ZIO[Scope, Throwable, GreyhoundSidecarUserClient.ZService[Any, Any]]] =
    channel(tenantId).map(GreyhoundSidecarUserClient.scoped(_))
}
