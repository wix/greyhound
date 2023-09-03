package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.TopicPartition
import zio.{Ref, Trace, URIO, ZIO}
import zio.test.junit.JUnitRunnableSpec
import zio.test._
import zio.test.Assertion._

class RebalanceListenerTest extends JUnitRunnableSpec {
  private implicit val trace = Trace.empty
  def spec                   = suite("RebalanceListenerTest")(
    test("*> operator") {
      for {
        loggedRef <- Ref.make(Vector.empty[String])
        log        = (s: String) => loggedRef.update(_ :+ s)
        runtime   <- ZIO.runtime[Any]
        unsafeLog  = (s: String) => zio.Unsafe.unsafe { implicit ss => runtime.unsafe.run(log(s)).getOrThrowFiberFailure() }
        listener   = (id: String) =>
                       new RebalanceListener[Any] {
                         override def onPartitionsRevoked(
                           consumer: Consumer,
                           partitions: Set[TopicPartition]
                         )(implicit trace: Trace): URIO[Any, DelayedRebalanceEffect] =
                           log(s"$id.revoke $partitions").as(DelayedRebalanceEffect(unsafeLog(s"$id.revoke.tle $partitions")))
                         override def onPartitionsAssigned(consumer: Consumer, partitions: Set[TopicPartition])(
                           implicit trace: Trace
                         ): URIO[Any, DelayedRebalanceEffect] =
                           log(s"$id.assigned $partitions").as(DelayedRebalanceEffect(unsafeLog(s"$id.assigned.tle $partitions")))
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
