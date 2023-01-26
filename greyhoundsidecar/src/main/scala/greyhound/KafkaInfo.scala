package greyhound

import zio.{ZIO, ZLayer}

trait KafkaInfo {
  val address: String
}

case class KafkaInfoLive(address: String) extends KafkaInfo

object KafkaInfoLive {

  val layer: ZLayer[Any, Nothing, KafkaInfoLive] = ZLayer.fromZIO {
    ZIO.attempt(scala.util.Properties.envOrNone("KAFKA_ADDRESS"))
      .flatMap {
        case Some(kafkaAddress) => ZIO.succeed(KafkaInfoLive(kafkaAddress))
        case None => ZIO.fail(new RuntimeException("kafka address not found"))
      }
      .orDie
  }
}
