package greyhound

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser._
import greyhound.Register.Register
import io.grpc.Status
import zio.ZIO

object ConsumerHandler {
  def apply(topic: String, group: String): Handler =
    RecordHandler { record: ConsumerRecord[String, String] =>

      SidecarUserClient.managed.flatMap(_.use { client =>

        val request = HandleMessagesRequest(
          topic = topic,
          group = group,
          records = Seq(Record(
            partition = record.partition,
            offset = record.offset,
            payload = Some(record.value),
            key = record.key)))

//        ZIO.fail(new RuntimeException("FAILING"))
        client.handleMessages(request)
          .tapBoth(
            fail => ZIO.succeed(println(s"~~~ CONSUME - send message failed: $fail")),
            _ => ZIO.succeed(println("CONSUME - SENT")))

      })

    }

  type Handler = RecordHandler[Register, Nothing, String, String]
}
