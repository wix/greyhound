package greyhound

import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest
import com.wixpress.dst.greyhound.testkit.ManagedKafkaConfig
import zio.ZIO

object Produce {

  val defaultKey: Option[String] = Some("")

  def apply(request: ProduceRequest) =
    Producer.make(config).use { producer =>
      producer.produce(
        record = ProducerRecord(
          topic = request.topic,
          value = request.payload.getOrElse(""),
          key = request.target.key orElse defaultKey),
        keySerializer = Serdes.StringSerde,
        valueSerializer = Serdes.StringSerde
      ).tapBoth(
          error => ZIO.succeed(println(s"~~~ PRODUCE ~~~ failed producing a msg, error [$error] request [$request]")),
          response => ZIO.succeed(println(s"~~~ PRODUCE ~~~ produced a msg! response [$response]")))
    }

  val bootstrapServer = s"localhost:${ManagedKafkaConfig.Default.kafkaPort}"
  val config = ProducerConfig(bootstrapServer)


}
