package greyhound

import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.KeepAliveRequest
import zio.{RIO, Scope, ZIO, durationInt}


trait KeepAliveScheduler {
  def run(): RIO[Env, Unit]
}

object KeepAliveScheduler {

  def apply(tenantId: String, hostDetails: TenantHostDetails, tenantRegistry: Registry): RIO[Env with Scope, Unit] = {
    for {
      client <- SidecarUserClient(hostDetails)
      _ <- new KeepAliveScheduler {
        override def run(): RIO[Env, Unit] = {
          for {
            _ <- ZIO.sleep(2.second)
            isAlive <- client.keepAlive(KeepAliveRequest()).foldZIO(_ => ZIO.succeed(false), _ => ZIO.succeed(true))

            maybeTenantInfo <- tenantRegistry.getTenant(tenantId)
            _ <- maybeTenantInfo match {
              case Some(tenantInfo) if !tenantInfo.hostDetails.alive && !isAlive =>
                for {
                  _ <- ZIO.foreach(tenantInfo.consumers.values)(_.shutdown)
                  _ <- tenantRegistry.removeTenant(tenantId)
                  _ <- ZIO.interrupt
                } yield ()
              case Some(_) =>
                tenantRegistry.markTenantStatusAs(tenantId, isAlive)
              case None =>
                ZIO.interrupt
            }
          } yield ()
        }
      }.run().forever
    } yield ()
  }
}
