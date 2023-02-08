package greyhound.sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RCGreyhoundSidecarUser
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse, KeepAliveRequest, KeepAliveResponse}
import io.grpc.Status
import zio.{IO, Ref, ZIO, ZLayer}

class FailOnceTestSidecarUser(consumedTopics: Ref[Seq[HandleMessagesRequest]],
                              shouldFailHandleMessage: Ref[Boolean],
                              shouldKeepAlive: Ref[Boolean]) extends RCGreyhoundSidecarUser {

  def collectedRequests = consumedTopics.get

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] = {
    for {
      shouldFail <- shouldFailHandleMessage.get
      _ <- if (shouldFail) {
        shouldFailHandleMessage.update(_ => false) *> ZIO.fail(Status.DATA_LOSS)
      } else {
        consumedTopics.update(_ :+ request)
      }
    } yield HandleMessagesResponse()
  }

  override def keepAlive(request: KeepAliveRequest): IO[Status, KeepAliveResponse] =
    shouldKeepAlive.get.flatMap {
      case true => ZIO.succeed(KeepAliveResponse())
      case false => ZIO.fail(Status.UNAVAILABLE)
    }

  def updateShouldKeepAlive(keepAlive: Boolean): ZIO[Any, Nothing, Unit] =
    shouldKeepAlive.update(_ => keepAlive)
}

object FailOnceTestSidecarUser {
  val layer: ZLayer[Any, Nothing, FailOnceTestSidecarUser] = ZLayer {
    for {
      messageSinkRef <- Ref.make[Seq[HandleMessagesRequest]](Nil)
      shouldFailHandleMessage <- Ref.make[Boolean](true)
      shouldFailKeepAlive <- Ref.make[Boolean](true)
    } yield new FailOnceTestSidecarUser(messageSinkRef, shouldFailHandleMessage, shouldFailKeepAlive)
  }
}
