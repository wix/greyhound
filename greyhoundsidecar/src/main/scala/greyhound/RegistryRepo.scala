package greyhound

import zio.Ref.Synchronized
import zio.{Task, UIO, ZIO, ZLayer}
import zio.redis.{CodecSupplier, Redis, RedisConfig, RedisError, RedisExecutor}

import scala.util.Try

trait RegistryRepo {
  def addTenant(tenantId: String, host: String, port: Int): Task[Unit]
}

case class RedisRegistryRepo(redis: Redis) extends RegistryRepo {
  override def addTenant(tenantId: String, host: String, port: Int): Task[Unit] = ???
}

//TODO separate into two files
case class InMemoryRegistryRepo(ref: Synchronized[Map[String, TenantInfo]]) extends RegistryRepo {
  override def addTenant(tenantId: String, host: String, port: Int): Task[Unit] = ???
}

object InMemoryRegistryRepo {
  lazy val layer: ZLayer[Any, Throwable, RegistryRepo] = ZLayer.fromZIO {
    for {
      ref <- Synchronized.make(Map.empty[String, TenantInfo])
    } yield InMemoryRegistryRepo(ref)
  }
}

object RedisRegistryRepo {
  lazy val layer: ZLayer[Redis, RedisError.IOError, RegistryRepo] = ZLayer.fromFunction(RedisRegistryRepo(_))
}