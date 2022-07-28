package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.TopicPartition
import zio.{Ref, URIO, ZIO}
import zio.test.junit.JUnitRunnableSpec
import zio.test._
import zio.test.Assertion._

class RebalanceListenerTest extends JUnitRunnableSpec {
  def spec = suite("RebalanceListenerTest")(
    testM("*> operator") {
      for {
        loggedRef <- Ref.make(Vector.empty[String])
        log        = (s: String) => loggedRef.update(_ :+ s)
        runtime   <- ZIO.runtime[Any]
        unsafeLog  = (s: String) => runtime.unsafeRunTask(log(s))
        listener   = (id: String) =>
                       new RebalanceListener[Any] {
                         override def onPartitionsRevoked(
                           consumer: Consumer,
                           partitions: Set[TopicPartition]
                         ): URIO[Any, DelayedRebalanceEffect] =
                           log(s"$id.revoke $partitions").as(DelayedRebalanceEffect(unsafeLog(s"$id.revoke.tle $partitions")))
                         override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition]): URIO[Any, Any] =
                           log(s"$id.assigned $partitions")
                       }
        l1l2       = listener("l1") *> listener("l2")
        partitions = Set(TopicPartition("topic", 0))

        _ <- l1l2.onPartitionsRevoked(null, partitions).map(_.run())
        _ <- l1l2.onPartitionsAssigned(null, partitions)

        logged <- loggedRef.get

      } yield {
        assert(logged)(
          equalTo(
            Seq(
              s"l1.revoke $partitions",
              s"l2.revoke $partitions",
              s"l1.revoke.tle $partitions",
              s"l2.revoke.tle $partitions",
              s"l1.assigned $partitions",
              s"l2.assigned $partitions"
            )
          )
        )
      }
    }
  )
}
