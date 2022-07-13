package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._

object CreateConsumer {

  def apply(topic: String, group: String) =
    for {
      kafkaAddress <- Register.get.map(_.kafkaAddress)
      _ <- RecordConsumer.make(
        config = RecordConsumerConfig(
          bootstrapServers = kafkaAddress,
          group = group,
          offsetReset = OffsetReset.Earliest,
          initialSubscription = ConsumerSubscription.Topics(Set(topic))),
        handler =
          ConsumerHandler(topic, group)
            .withDeserializers(Serdes.StringSerde, Serdes.StringSerde)
      ).useForever
    } yield ()

}
