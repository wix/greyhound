package greyhound.support

import greyhound.{InMemoryRegistryRepo, KafkaInfo, RedisRegistryRepo, RegistryRepo, TenantRegistry}
import zio.ZLayer
import zio.redis.{CodecSupplier, Redis, RedisExecutor}
import zio.redis.embedded.EmbeddedRedis

trait ConnectionSettings {
  val kafkaPort: Int
  val zooKeeperPort: Int
  val sideCarUserGrpcPort: Int
  val isStandaloneMode: Boolean

  final val localhost: String = "localhost"

  case class TestKafkaInfo() extends KafkaInfo {
    override val address: String = s"$localhost:$kafkaPort"
  }

  //TODO move to test context?
  object TestKafkaInfo {
    val layer = ZLayer.succeed(TestKafkaInfo())
  }

  object TestRegistryRepo {
    def layer = if (isStandaloneMode) redisTestLayer else inMemoryTestLayer

    val redisTestLayer = ZLayer.make[RegistryRepo](
      RedisRegistryRepo.layer,
      Redis.layer,
      RedisExecutor.layer,
      EmbeddedRedis.layer,
      ZLayer.succeed(CodecSupplier.utf8string),
    )

    val inMemoryTestLayer = InMemoryRegistryRepo.layer
  }

  object TestTenantRegistry {
    val layer = ZLayer.make[TenantRegistry](
      TenantRegistry.layer,
      TestRegistryRepo.layer
    )
  }

  //TODO also add sidecar service layer?
}
