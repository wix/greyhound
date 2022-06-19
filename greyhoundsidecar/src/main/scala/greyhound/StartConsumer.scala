package greyhound

import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.core.Serdes._
import zio._

//object StartConsumer {
//
//  val handler: RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]] = ???
//
//  def apply(topic: String, group: String) = for {
//    host <- Register.get
//    _ <- RecordConsumer.make(
//      RecordConsumerConfig(
//        Produce.bootstrapServer,
//        group,
//        ConsumerSubscription.Topics(Set(topic))),
//      handler.withDeserializers(StringSerde, StringSerde))
//      .useForever
//  } yield ()
//}
