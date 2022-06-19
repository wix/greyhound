package greyhound

import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest
import com.wixpress.dst.greyhound.testkit.ManagedKafkaConfig

object Produce {

  def apply(request: ProduceRequest) =
    Producer.make(config).use { producer =>
      producer.produce(
        record = ProducerRecord(
          topic = request.topic,
          value = request.payload.getOrElse(""),
          key = request.target.key),
        keySerializer = Serdes.StringSerde,
        valueSerializer = Serdes.StringSerde)
    }

  val bootstrapServer = s"localhost:${ManagedKafkaConfig.Default.kafkaPort}"
  val config = ProducerConfig(bootstrapServer)


}
