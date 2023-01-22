package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.producer.Producer.Producer
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest
import zio.{RIO, Scope, ZIO}

object Produce {

  val defaultKey: Option[String] = Some("")

  def apply(request: ProduceRequest, producer: Producer): RIO[Scope, Unit] = {
    val record = ProducerRecord(topic = request.topic, value = request.payload.getOrElse(""), key = request.target.key orElse defaultKey)
    for {
      recordMetadata <- producer.produce(
        record = record,
        keySerializer = Serdes.StringSerde,
        valueSerializer = Serdes.StringSerde
      )
      _ <- ZIO.logDebug(s"Successfully produced - topic: ${recordMetadata.topic} partition: ${recordMetadata.partition} offset: ${recordMetadata.offset}")
    } yield ()
  }

}
