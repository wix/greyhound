package com.wixpress.dst.greyhound.core.producer

import com.wixpress.dst.greyhound.core.{PartitionInfo, Topic}
import com.wixpress.dst.greyhound.core.producer.ProducerCombinatorsTest.SomeEnv
import com.wixpress.dst.greyhound.core.testkit.FakeProducer
import zio.test._
import zio.test.Assertion._
import zio.test.junit.JUnitRunnableSpec
import zio.{Chunk, IO, RIO, Ref, Trace, UIO, ZEnvironment, ZIO}

class ProducerCombinatorsTest extends JUnitRunnableSpec {
  private implicit val trace = Trace.empty
  def spec = suite("ProducerCombinatorsTest")(
    test("provide combinator") {
      for {
        log        <- Ref.make(List.empty[String])
        producerR   = new ProducerR[SomeEnv] {
                        override def produceAsync(
                          record: ProducerRecord[Chunk[Byte], Chunk[Byte]]
                        )(implicit trace: Trace): ZIO[SomeEnv, ProducerError, IO[ProducerError, RecordMetadata]] =
                          ZIO.environment[SomeEnv].map { env =>
                            log.update(_ :+ s"produce:${env.get.env}:${record.topic}").as(RecordMetadata(record.topic, 0, 0))
                          }

                        override def shutdown (implicit trace: Trace): UIO[Unit] = log.update(_ :+ "shutdown")

                        override def attributes: Map[String, String] = Map("atr1" -> "val1")

                        override def partitionsFor(topic: Topic) (implicit trace: Trace): RIO[Any, Seq[PartitionInfo]] =
                          ZIO.succeed((1 to 3) map (p => PartitionInfo(topic, p, 1)))
                      }
        producer    = producerR.provide(ZEnvironment(SomeEnv("the-env")))
        _          <- producer.produce(ProducerRecord("topic1", Chunk.empty))
        _          <- producer.shutdown
        logged     <- log.get
        partitions <- producer.partitionsFor("some-topic")
      } yield {
        assert(logged)(
          equalTo(
            List(
              "produce:the-env:topic1",
              "shutdown"
            )
          )
        ) && assert(producer.attributes)(equalTo(Map("atr1" -> "val1"))) &&
        assert(partitions)(equalTo((1 to 3) map (p => PartitionInfo("some-topic", p, 1))))
      }
    },
    test("tapBoth combinator") {
      val error = ProducerError.from(new RuntimeException())
      for {
        log        <- Ref.make(List.empty[String])
        shouldFail <- Ref.make(false)
        original   <- FakeProducer.make(
                        beforeComplete = ZIO.whenZIO(shouldFail.get)(ZIO.fail(error)).as(_),
                        attributes = Map("atr1" -> "val1"),
                        onShutdown = log.update(_ :+ "shutdown")
                      )
        producer    = original.tapBoth(
                        (topic, e) => log.update(_ :+ s"error:$topic:${e.squash.getClass.getSimpleName}"),
                        rmd => log.update(_ :+ s"produce:${rmd.topic}")
                      )
        success    <- producer.produce(ProducerRecord("topic1", Chunk.empty))
        _          <- shouldFail.set(true)
        failed     <- producer.produce(ProducerRecord("topic2", Chunk.empty)).either
        _          <- producer.shutdown
        logged     <- log.get
      } yield {
        assert(logged)(
          equalTo(
            List(
              "produce:topic1",
              "error:topic2:UnknownError",
              "shutdown"
            )
          )
        ) && assert(producer.attributes)(equalTo(Map("atr1" -> "val1"))) && assert(failed)(isLeft(equalTo(error))) &&
        assert(success)(equalTo(RecordMetadata("topic1", 0, 0)))
      }
    },
    test("onShutdown") {
      for {
        log      <- Ref.make(List.empty[String])
        original <- FakeProducer.make(
                      onShutdown = log.update(_ :+ "shutdown")
                    )
        producer  = original.onShutdown(log.update(_ :+ "onShutdown"))
        _        <- producer.shutdown
        logged   <- log.get
      } yield {
        assert(logged)(
          equalTo(
            List(
              "onShutdown",
              "shutdown"
            )
          )
        )
      }
    }
  )
}

object ProducerCombinatorsTest {
  case class SomeEnv(env: String)
}
