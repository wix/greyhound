package greyhound

import zio.{ZIO, ZLayer}

trait KafkaInfo {
  val address: String
}

case class KafkaInfoLive(address: String) extends KafkaInfo

object KafkaInfoLive {

  val layer: ZLayer[Any, Nothing, KafkaInfoLive] = ZLayer.fromZIO {
    ZIO.attempt(scala.util.Properties.envOrElse("KAFKA_ADDRESS", "kafka:29092"))
      .orDie
      .map(KafkaInfoLive(_))
  }
}
