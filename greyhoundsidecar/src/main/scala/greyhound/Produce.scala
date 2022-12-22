package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest
import zio.{RIO, Scope, UIO, ZIO}

object Produce {

  val defaultKey: Option[String] = Some("")

  def apply(request: ProduceRequest, kafkaAddress: String, onProduceListener: ProducerRecord[_, _] => UIO[Unit]): RIO[Scope, Unit] = {
    for {
      producer <- Producer.make(ProducerConfig(kafkaAddress, onProduceListener = onProduceListener)) // TODO: memoize or pass
      record = ProducerRecord(topic = request.topic, value = request.payload.getOrElse(""), key = request.target.key orElse defaultKey)
      recordMetadata <- producer.produce(
        record = record,
        keySerializer = Serdes.StringSerde,
        valueSerializer = Serdes.StringSerde
      )
      _ <- ZIO.logDebug(s"Successfully produced - topic: ${recordMetadata.topic} partition: ${recordMetadata.partition} offset: ${recordMetadata.offset}")
    } yield ()
  }

}
