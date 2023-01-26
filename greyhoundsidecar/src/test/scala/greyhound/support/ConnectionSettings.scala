package greyhound.support

import greyhound.KafkaInfo
import zio.ZLayer

trait ConnectionSettings {
  val kafkaPort: Int
  val zooKeeperPort: Int
  val sideCarUserGrpcPort: Int

  final val localhost: String = "localhost"

  case class TestKafkaInfo() extends KafkaInfo {
    override val address: String = s"$localhost:$kafkaPort"
  }

  object TestKafkaInfo {
    val layer = ZLayer.succeed(TestKafkaInfo())
  }
}
