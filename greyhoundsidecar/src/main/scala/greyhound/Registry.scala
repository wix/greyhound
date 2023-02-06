package greyhound

import zio.{Ref, Task, UIO, ZLayer}

trait Registry {

  def addTenant(tenantId: String, host: String, port: Int): Task[Unit]

  def removeTenant(tenantId: String): Task[Unit]

  def getTenant(tenantId: String): UIO[Option[TenantInfo]]

  def tenantExists(tenantId: String): Task[Boolean]

  def addConsumer(tenantId: String, topic: String, consumerGroup: String): Task[Unit]

  def removeConsumer(tenantId: String, topic: String, consumerGroup: String): Task[Unit]

  def getConsumer(tenantId: String, topic: String, consumerGroup: String): UIO[Option[TenantConsumerInfo]]
}
case class TenantRegistry(ref: Ref[Map[String, TenantInfo]]) extends Registry {
  override def addTenant(tenantId: String, host: String, port: Int): Task[Unit] =
    ref.update(_.updated(tenantId, TenantInfo(hostDetails = TenantHostDetails(host, port), consumers = Map.empty)))

  override def removeTenant(tenantId: String): Task[Unit] =
    ref.update(_ - tenantId)

  override def getTenant(tenantId: String): UIO[Option[TenantInfo]] =
    ref.get.map(_.get(tenantId))

  override def addConsumer(tenantId: String, topic: String, consumerGroup: String): Task[Unit] =
    ref.update { tenants =>
      tenants.get(tenantId) match {
        case Some(tenant) =>
          tenants.updated(tenantId, tenant.copy(consumers = tenant.consumers.updated((topic, consumerGroup), TenantConsumerInfo(topic, consumerGroup))))
        case None =>
          tenants
      }
    }

  override def removeConsumer(tenantId: String, topic: String, consumerGroup: String): Task[Unit] =
    ref.update { tenants =>
      tenants.get(tenantId) match {
        case Some(tenant) =>
          tenants.updated(tenantId, tenant.copy(consumers = tenant.consumers - ((topic, consumerGroup))))
        case None =>
          tenants
      }
    }

  override def getConsumer(tenantId: String, topic: String, consumerGroup: String): UIO[Option[TenantConsumerInfo]] =
    ref.get.map(_.get(tenantId).flatMap(_.consumers.get((topic, consumerGroup))))

  override def tenantExists(tenantId: String): Task[Boolean] =
    ref.get.map(_.contains(tenantId))
}

object TenantRegistry {
  val layer = ZLayer.fromZIO {
    Ref.make(Map.empty[String, TenantInfo])
      .map(TenantRegistry(_))
  }
}

case class TenantInfo(hostDetails: TenantHostDetails, consumers: Map[(String, String), TenantConsumerInfo])

case class TenantHostDetails(host: String, port: Int)

case class TenantConsumerInfo(topic: String, consumerGroup: String)
//                              , shutdown: RIO[Env, Unit])

