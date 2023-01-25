package greyhound.sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RCGreyhoundSidecarUser
import io.grpc.Status
import zio.{Ref, ZIO}

class TestSidecarUser(consumedTopics: Ref[Seq[HandleMessagesRequest]]) extends RCGreyhoundSidecarUser {

  def collectedRequests = consumedTopics.get

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] =
      consumedTopics.update(_ :+ request)
        .as(HandleMessagesResponse())
}