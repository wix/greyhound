package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import greyhound.Register.Register
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scalapb.zio_grpc.{ZChannel, ZManagedChannel}
import zio.{Scope, ZIO}

object SidecarUserClient extends {


  val channel: ZIO[Register, Nothing, ZIO[Scope, Throwable, ZChannel[Any]]] = Register.get.map{ db =>
    ZManagedChannel[ZIO[Scope, Throwable, ZChannel[Any]]](
      ManagedChannelBuilder
        .forAddress(db.host.host, db.host.port)
        .usePlaintext()
    )

  }

  val managed = channel.map(cn=> GreyhoundSidecarUserClient.scoped(cn))
}
