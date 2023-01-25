package greyhound

import com.wixpress.dst.greyhound.core.consumer.domain.{BatchRecordHandler, ConsumerRecordBatch, HandleError}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.GreyhoundSidecarUserClient
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser._
import io.grpc.Status
import zio.ZIO

object BatchConsumerHandler {
  def apply(topic: String, group: String, client: GreyhoundSidecarUserClient.ZService[Any]): Handler =
    BatchRecordHandler { recordBatch: ConsumerRecordBatch[String, String] =>
      val request = HandleMessagesRequest(
        topic = topic,
        group = group,
        records = recordBatch.records.map { record =>
          Record(partition = record.partition, offset = record.offset, payload = Some(record.value), key = record.key)
        }
      )

      client
        .handleMessages(request)
        .tapBoth(
          e => ZIO.logError(s"Failed to send msg to client: $e"),
          f => ZIO.logDebug(s"Successfully sent msg to client: $f"))
        .catchAll(grpcStatus => ZIO.fail(HandleError(BatchConsumerFailure(grpcStatus))))
    }

  type Handler = BatchRecordHandler[Any, Throwable, String, String]

  case class BatchConsumerFailure(grpcStatus: Status) extends RuntimeException
}
