package greyhound

import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.KeepAliveRequest
import zio.{RIO, Schedule, Scope, ZIO, durationInt}

object KeepAliveScheduler {

  def apply(tenantId: String, hostDetails: TenantHostDetails, tenantRegistry: Registry): RIO[Env with Scope, Unit] = {
    for {
      client <- SidecarUserClient(hostDetails)
      action = for {
        isAlive <- client.keepAlive(KeepAliveRequest()).foldZIO(_ => ZIO.succeed(false), _ => ZIO.succeed(true))
        _ <- tenantRegistry.removeDeadTenant(tenantId, isAlive)
        _ <- tenantRegistry.markTenantStatusAs(tenantId, isAlive)
      } yield tenantId

      policy = Schedule.recurUntilZIO[Env, String](tenantId => tenantRegistry.getTenant(tenantId).map(_.isEmpty)) && Schedule.spaced(2.seconds)
     _ <- action repeat policy
    } yield ()
  }
}
