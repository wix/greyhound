package greyhound

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerRecord, RecordHandler}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser._
import io.grpc.Status
import zio.ZIO

object ConsumerHandler {
  def apply(topic: String, group: String, client: GreyhoundSidecarUserClient.ZService[Any, Any]): Handler =
    RecordHandler { record: ConsumerRecord[String, String] =>
      val request = HandleMessagesRequest(
        topic = topic,
        group = group,
        records = Seq(Record(partition = record.partition, offset = record.offset, payload = Some(record.value), key = record.key))
      )
      println("message consumed")
      client
        .handleMessages(request)
        .catchAll(grpcStatus => ZIO.fail(ConsumerFailure(grpcStatus)))
    }

  type Handler = RecordHandler[Any, Throwable, String, String]

  case class ConsumerFailure(grpcStatus: Status) extends RuntimeException
}
