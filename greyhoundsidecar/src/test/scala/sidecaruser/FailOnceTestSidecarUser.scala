package sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RGreyhoundSidecarUser
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse}
import io.grpc.Status
import zio.{Ref, ZIO}

class FailOnceTestSidecarUser(consumedTopics: Ref[Seq[HandleMessagesRequest]],
                              shouldFailRef: Ref[Boolean]) extends RGreyhoundSidecarUser[Any] {

  def collectedRecords = consumedTopics.get

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] = {
    for {
      shouldFail <- shouldFailRef.get
      _ <- if (shouldFail)  {
        shouldFailRef.update(_ => false) *> ZIO.fail(Status.DATA_LOSS)
      } else  {
        consumedTopics.update(_ :+ request)
      }
    } yield HandleMessagesResponse()
  }
}
