package sidecaruser

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse}
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RGreyhoundSidecarUser
import io.grpc.Status
import zio.{Ref, ZIO}

import java.util.concurrent.TimeUnit

class FailOnceSidecarUserService(consumedTopics: Ref[Seq[HandleMessagesRequest]], shouldFailRef: Ref[Boolean]) extends RGreyhoundSidecarUser[Any] {


  def collectedRecords: Ref[Seq[HandleMessagesRequest]] = consumedTopics

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] = {
    for {
      shouldFail <- shouldFailRef.get
      _ <- if (shouldFail)
        shouldFailRef.updateAndGet(_ => false).flatMap(
          updatedShouldFail => log("Failed", request.topic, shouldFail, updatedShouldFail)) *> ZIO.fail(Status.DATA_LOSS) else consumedTopics.update(_ :+ request)
      shouldFailGetAfter <- shouldFailRef.get
      _ <- log("Succeeded", request.topic, shouldFail, shouldFailGetAfter)
    } yield HandleMessagesResponse()
  }

  private def log(status: String, topic: String, shouldFail: Boolean, updatedShouldFail: Boolean) = {
    for {
      time <- zio.Clock.currentTime(TimeUnit.MILLISECONDS)
      _ <- zio.Console.printLine(s"!! ${status} consume for topic ${topic}, clock time: ${time}, original shouldFail: ${shouldFail}, the next time shouldFail is: ${updatedShouldFail} !!").orDie
    } yield ()
  }
}
