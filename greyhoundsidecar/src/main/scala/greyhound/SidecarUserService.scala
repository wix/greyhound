package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse, Record}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RGreyhoundSidecarUser
import io.grpc.Status
import zio.console.putStrLn
import zio.{ZEnv, ZIO}

class SidecarUserService extends RGreyhoundSidecarUser[ZEnv]{

  override def handleMessages(request: HandleMessagesRequest): ZIO[ZEnv, Status, HandleMessagesResponse] = {
    putStrLn(
      s"""~~~ Sidecar User CONSUMING ~~~
         |topic = ${request.topic}
         |group = ${request.group}
         |records =
         |${printRecords(request.records)}
         |~~~ End Message ~~~
         |""".stripMargin
    ).orDie *>
//      ZIO.succeed(HandleMessagesResponse())
      ZIO.fail(new RuntimeException("Failed to consume")).mapError(Status.fromThrowable)
  }

  private def printRecords(records: Seq[Record]) =
    records.map(record =>
      s"""  key = ${record.key}
         |  payload = ${record.payload}
         |  headers = ${record.headers}
         |  offset = ${record.offset}
         |""".stripMargin).mkString

}
