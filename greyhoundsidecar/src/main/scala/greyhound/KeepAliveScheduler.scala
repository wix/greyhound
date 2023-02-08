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
            _ <- ZIO.sleep(1.second)
            response <- client.keepAlive(KeepAliveRequest()).either
            _ <- response match {
              case Right(_) =>
                tenantRegistry.markTenantAsDead(tenantId)
              case Left(_) =>
                ZIO.unit
            }
            maybeTenantInfo <- tenantRegistry.getTenant(tenantId)
            _ <- maybeTenantInfo match {
              case Some(tenantInfo) if tenantInfo.hostDetails.alive =>
                tenantRegistry.markTenantAsDead(tenantId)
              case Some(tenantInfo) if !tenantInfo.hostDetails.alive =>
                for {
                  _ <- ZIO.foreach(tenantInfo.consumers.values)(_.shutdown)
                  _ <- tenantRegistry.removeTenant(tenantId)
                  _ <- ZIO.interrupt
                } yield ()
              case None =>
                ZIO.interrupt
            }
          } yield ()
        }
      }.run().forever
    } yield ()
  }
}
