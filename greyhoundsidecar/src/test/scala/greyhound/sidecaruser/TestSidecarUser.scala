package greyhound.sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse, KeepAliveRequest, KeepAliveResponse}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RCGreyhoundSidecarUser
import io.grpc.Status
import zio.{IO, Ref, ZIO, ZLayer}

class TestSidecarUser(consumedTopics: Ref[Seq[HandleMessagesRequest]],
                      shouldKeepAlive: Ref[Boolean]) extends RCGreyhoundSidecarUser {

  def collectedRequests = consumedTopics.get

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] =
    consumedTopics.update(_ :+ request)
      .as(HandleMessagesResponse())

  override def keepAlive(request: KeepAliveRequest): IO[Status, KeepAliveResponse] =
    shouldKeepAlive.get.flatMap {
      case true => ZIO.succeed(KeepAliveResponse())
      case false => ZIO.fail(Status.UNAVAILABLE)
    }

  def updateShouldKeepAlive(keepAlive: Boolean): ZIO[Any, Nothing, Unit] =
    shouldKeepAlive.update(_ => keepAlive)
}

object TestSidecarUser {
  val layer: ZLayer[Any, Nothing, TestSidecarUser] = ZLayer {
    for {
      messageSinkRef <- Ref.make[Seq[HandleMessagesRequest]](Nil)
      shouldFailKeepAlive <- Ref.make[Boolean](true)
    } yield new TestSidecarUser(messageSinkRef, shouldFailKeepAlive)
  }
}
