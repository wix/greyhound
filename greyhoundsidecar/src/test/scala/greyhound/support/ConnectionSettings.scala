package greyhound.support

import greyhound.{KafkaInfo, RegistryRepo, TenantRegistry}
import zio.ZLayer
import zio.redis.{CodecSupplier, Redis, RedisExecutor}
import zio.redis.embedded.EmbeddedRedis

trait ConnectionSettings {
  val kafkaPort: Int
  val zooKeeperPort: Int
  val sideCarUserGrpcPort: Int

  final val localhost: String = "localhost"

  case class TestKafkaInfo() extends KafkaInfo {
    override val address: String = s"$localhost:$kafkaPort"
  }

  //TODO move to test context?
  object TestKafkaInfo {
    val layer = ZLayer.succeed(TestKafkaInfo())
  }

  object TestRegistryRepo {
    val layer = ZLayer.make[RegistryRepo](
      RegistryRepo.layer,
      Redis.layer,
      RedisExecutor.layer,
      EmbeddedRedis.layer,
      ZLayer.succeed(CodecSupplier.utf8string),
    )
  }

  object TestTenantRegistry {
    val layer = ZLayer.make[TenantRegistry](
      TenantRegistry.layer,
      TestRegistryRepo.layer
    )
  }

  //TODO also add sidecar service layer?
}
