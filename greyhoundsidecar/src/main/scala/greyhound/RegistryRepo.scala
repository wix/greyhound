package greyhound

import zio.{Task, ZLayer}
import zio.redis.Redis

trait RegistryRepo {
  def addTenant(tenantId: String, host: String, port: Int): Task[Unit]
}

case class RedisRegistryRepo(redis: Redis) extends RegistryRepo {
  override def addTenant(tenantId: String, host: String, port: Int): Task[Unit] = ???
}

object RegistryRepo {
  val layer: ZLayer[Redis, Nothing, RedisRegistryRepo] = ZLayer.fromFunction(RedisRegistryRepo(_))
}