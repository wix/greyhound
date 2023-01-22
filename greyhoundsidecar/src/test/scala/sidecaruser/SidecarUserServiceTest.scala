package sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RGreyhoundSidecarUser
import io.grpc.Status
import zio.{Ref, ZIO}

class SidecarUserServiceTest(consumedTopics: Ref[Seq[HandleMessagesRequest]]) extends RGreyhoundSidecarUser[Any] {

  def collectedRecords: Ref[Seq[HandleMessagesRequest]] = consumedTopics

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] = {
    zio.Console.printLine("!!!!!          consume          !!!!!").orElse(ZIO.fail(Status.RESOURCE_EXHAUSTED)) *> consumedTopics.update(_ :+ request) *>  ZIO.succeed(HandleMessagesResponse())
  }


}

class FailOneSidecarUserServiceTest(consumedTopics: Ref[Seq[HandleMessagesRequest]]) extends RGreyhoundSidecarUser[Any] {

  var fail = true

  def collectedRecords: Ref[Seq[HandleMessagesRequest]] = consumedTopics

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] =
    if (fail) {
      fail = false
      ZIO.fail(Status.DATA_LOSS)
    } else {
      ZIO.succeed(HandleMessagesResponse())
    }
}
