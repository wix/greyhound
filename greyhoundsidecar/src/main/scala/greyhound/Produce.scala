package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest
import zio.{Task, ZIO}

object Produce {

  val defaultKey: Option[String] = Some("")

  def apply(request: ProduceRequest, kafkaAddress: String): Task[Unit] = {
    ZIO.scoped {
      for {
        producer <- Producer.make(ProducerConfig(kafkaAddress))
        _ <- producer.produce(
          record = ProducerRecord(topic = request.topic, value = request.payload.getOrElse(""), key = request.target.key orElse defaultKey),
          keySerializer = Serdes.StringSerde,
          valueSerializer = Serdes.StringSerde
        )
      } yield ()
    }
  }
}
