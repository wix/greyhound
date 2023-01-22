package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import greyhound.Register.Register
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scalapb.zio_grpc.{ZChannel, ZManagedChannel}
import zio.{Scope, ZIO}

object SidecarUserClient extends {


  val channel: ZIO[Register, Nothing, ZManagedChannel[Any]] = Register.get.map { hostDetails =>
    // this val construction in needed for IntelliJ to understand the type - god knows why???
    val managedChannel: ZManagedChannel[Any] = ZManagedChannel[Any](
      ManagedChannelBuilder
        .forAddress(hostDetails.host, hostDetails.port)
        .usePlaintext()
    )

    managedChannel
  }

  val managed: ZIO[Register, Nothing, ZIO[Scope, Throwable, GreyhoundSidecarUserClient.ZService[Any, Any]]] = channel.map(channel => GreyhoundSidecarUserClient.scoped(channel))
}
