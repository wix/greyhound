package greyhound

import com.wixpress.dst.greyhound.core.consumer.RecordConsumer.Env
import io.grpc.{Status, StatusRuntimeException}
import zio.Ref.Synchronized
import zio.{RIO, Task, UIO, ZIO, ZLayer}

trait Registry {

  def addTenant(tenantId: String, host: String, port: Int): Task[Unit]

  def handleTenantActivityStatus(tenantId: String, isAlive: Boolean): RIO[Env, Unit]

  def getTenant(tenantId: String): UIO[Option[TenantInfo]]

  def tenantExists(tenantId: String): Task[Boolean]

  def addConsumer(tenantId: String, topic: String, consumerGroup: String, shutdown: RIO[Env, Unit]): Task[Unit]

  def removeConsumer(tenantId: String, topic: String, consumerGroup: String): RIO[Env, Unit]

  def getConsumer(tenantId: String, topic: String, consumerGroup: String): UIO[Option[TenantConsumerInfo]]

  def isUniqueConsumer(topic: String, consumerGroup: String, tenantId: String): UIO[Boolean]
}

case class TenantRegistry(ref: Synchronized[Map[String, TenantInfo]]) extends Registry {
  override def addTenant(tenantId: String, host: String, port: Int): Task[Unit] = {
    ref.updateZIO(tenants => if (tenants.values.map(_.hostDetails).exists(hostDetails => hostDetails.host == host && hostDetails.port == port))
      ZIO.fail(new StatusRuntimeException(Status.ALREADY_EXISTS))
    else
      ZIO.succeed(tenants.updated(tenantId, TenantInfo(hostDetails = TenantHostDetails(host, port, alive = true), consumers = Map.empty)))
    )
  }

  override def handleTenantActivityStatus(tenantId: String, isAlive: Boolean): RIO[Env, Unit] =
    ref.modifyZIO(tenants => tenants.get(tenantId) match {
      case Some(tenant) if !tenant.hostDetails.alive && !isAlive =>
        for {
          _ <- ZIO.foreachPar(tenant.consumers.values)(_.shutdown)
        } yield ((), tenants - tenantId)
      case Some(tenant) =>
        ZIO.succeed(((), tenants.updated(tenantId, tenant.copy(hostDetails = tenant.hostDetails.copy(alive = isAlive)))))
      case _ =>
        ZIO.succeed(((), tenants))
    })

  override def getTenant(tenantId: String): UIO[Option[TenantInfo]] =
    ref.get.map(_.get(tenantId))

  override def addConsumer(tenantId: String, topic: String, consumerGroup: String, shutdown: RIO[Env, Unit]): Task[Unit] =
    ref.update { tenants =>
      tenants.get(tenantId) match {
        case Some(tenant) =>
          tenants.updated(tenantId, tenant.copy(consumers = tenant.consumers.updated((topic, consumerGroup), TenantConsumerInfo(topic, consumerGroup, shutdown))))
        case None =>
          tenants
      }
    }

  override def removeConsumer(tenantId: String, topic: String, consumerGroup: String): RIO[Env, Unit] =
    ref.modifyZIO(tenants =>
      tenants.get(tenantId) match {
        case Some(tenant) =>
          for {
            _ <- tenant.consumers.get((topic, consumerGroup)).map(_.shutdown).getOrElse(ZIO.unit)
          } yield ((), tenants.updated(tenantId, tenant.copy(consumers = tenant.consumers - ((topic, consumerGroup)))))
        case None =>
          ZIO.succeed(((), tenants))
      })

  override def getConsumer(tenantId: String, topic: String, consumerGroup: String): UIO[Option[TenantConsumerInfo]] =
    ref.get.map(_.get(tenantId).flatMap(_.consumers.get((topic, consumerGroup))))

  override def tenantExists(tenantId: String): Task[Boolean] =
    ref.get.map(_.contains(tenantId))

  override def isUniqueConsumer(topic: String, consumerGroup: String, tenantId: String): UIO[Boolean] =
    ref.get.map(!_.get(tenantId).exists(_.consumers.contains((topic, consumerGroup))))
}

object TenantRegistry {
  val layer = ZLayer.fromZIO {
    Synchronized.make(Map.empty[String, TenantInfo])
      .map(TenantRegistry(_))
  }
}

case class TenantInfo(hostDetails: TenantHostDetails, consumers: Map[(String, String), TenantConsumerInfo])

case class TenantHostDetails(host: String, port: Int, alive: Boolean)

case class TenantConsumerInfo(topic: String, consumerGroup: String, shutdown: RIO[Env, Unit])

