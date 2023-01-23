package support

trait ConnectionSettings {
  val kafkaPort: Int
  val zooKeeperPort: Int
  val sideCarUserGrpcPort: Int

  final val localhost: String = "localhost"
  def kafkaAddress: String = s"$localhost:$kafkaPort"
}
