package greyhound

import zio.{UIO, ZIO}

object EnvArgs {
  val kafkaAddress: UIO[Option[String]] = ZIO.attempt {
    scala.util.Properties.envOrNone("KAFKA_ADDRESS")
  }.orDie
}
