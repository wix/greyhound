package greyhound.sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RCGreyhoundSidecarUser
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse}
import io.grpc.Status
import zio.{Ref, ZIO, ZLayer}

class FailOnceTestSidecarUser(consumedTopics: Ref[Seq[HandleMessagesRequest]],
                              shouldFailRef: Ref[Boolean]) extends RCGreyhoundSidecarUser {

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

object FailOnceTestSidecarUser {
  val layer: ZLayer[Any, Nothing, FailOnceTestSidecarUser] = ZLayer {
    for {
      messageSinkRef <- Ref.make[Seq[HandleMessagesRequest]](Nil)
      shouldFailRef <- Ref.make[Boolean](true)
    } yield new FailOnceTestSidecarUser(messageSinkRef, shouldFailRef)
  }
}
