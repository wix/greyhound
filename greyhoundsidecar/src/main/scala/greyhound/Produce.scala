package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest
import zio.{Scope, Task, RIO}

object Produce {

  val defaultKey: Option[String] = Some("")

  def apply(request: ProduceRequest, kafkaAddress: String): RIO[Scope, Unit] = {
      for {
        _ <- zio.Console.printLine("==============  trying to create producer =============")
        producer <- Producer.make(ProducerConfig(kafkaAddress))
        record = ProducerRecord(topic = request.topic, value = request.payload.getOrElse(""), key = request.target.key orElse defaultKey)
        _ <- zio.Console.printLine(s"+++++++++++++++  done to create producer +++++++++++++++ \n$record")

        _ <- producer.produce(
          record = record,
          keySerializer = Serdes.StringSerde,
          valueSerializer = Serdes.StringSerde
        )
      } yield ()
    }

}
