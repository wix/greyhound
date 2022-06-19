package greyhound

import zio.{UIO, ZIO}

object EnvArgs {
  val kafkaAddress: UIO[Option[String]] = ZIO.effect {
    scala.util.Properties.envOrNone("KAFKA_ADDRESS")
  }.orDie
}
