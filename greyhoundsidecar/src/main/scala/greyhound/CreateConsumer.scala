package greyhound

import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._
import zio.{Chunk, ZIO}

object CreateConsumer {

  def apply(topic: String, group: String) =
    RecordConsumer.make(
      config = RecordConsumerConfig(
        bootstrapServers = Produce.bootstrapServer,
        group = group,
        offsetReset = OffsetReset.Earliest,
        initialSubscription = ConsumerSubscription.Topics(Set(topic))),
      handler =
        ConsumerHandler(topic, group)
          .withDeserializers(Serdes.StringSerde, Serdes.StringSerde)
    ).useForever

  val dummyHandler: RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]] = RecordHandler { record =>
    ZIO.succeed(println("~~~ DUMMY HANDLER ~~~")) *> ZIO.succeed(record.value)
  }

}
