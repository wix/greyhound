package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.serialization.Deserializer
import com.wixpress.dst.greyhound.core.testkit.MessagesSink
import com.wixpress.dst.greyhound.core.{Headers, Record, Topic}
import org.apache.kafka.common.serialization.StringDeserializer
import org.specs2.mutable.SpecificationWithJUnit
import zio.duration._
import zio.stream.Sink.collectAllN
import zio.{DefaultRuntime, Ref, ZIO, clock}

class ParallelRecordHandlerTest
  extends SpecificationWithJUnit
    with DefaultRuntime {

  "handle record by topic" in unsafeRun {
    val group = "some-group"
    val topic1 = Topic[String, Foo]("topic1")
    val topic2 = Topic[String, Bar]("topic2")
    val stringDeserializer = Deserializer(new StringDeserializer)

    for {
      sink1 <- Ref.make(List.empty[Foo])
      spec1 <- ConsumerSpec.make[Any, String, Foo](
        topic = topic1,
        group = group,
        handler = RecordHandler(record => sink1.update(record.value :: _)),
        keyDeserializer = stringDeserializer,
        valueDeserializer = stringDeserializer.map(Foo))

      sink2 <- Ref.make(List.empty[Bar])
      spec2 <- ConsumerSpec.make[Any, String, Bar](
        topic = topic2,
        group = group,
        handler = RecordHandler(record => sink2.update(record.value :: _)),
        keyDeserializer = stringDeserializer,
        valueDeserializer = stringDeserializer.map(Bar))

      handler = ParallelRecordHandler(Map(spec1.topic -> spec1, spec2.topic -> spec2))
      _ <- handler.handle(Record(topic1.name, 0, 0L, Headers.Empty, None, "foo".getBytes)) *>
        handler.handle(Record(topic2.name, 0, 0L, Headers.Empty, None, "bar".getBytes))

      messages1 <- sink1.get
      messages2 <- sink2.get
    } yield (messages1 must contain(Foo("foo"))) and (messages2 must contain(Bar("bar")))
  }

  "parallelize handling based on partition" in unsafeRun {
    val topic = "some-topic"
    val partitions = 8

    for {
      sink <- MessagesSink.make[String, String]()
      slowHandler = sink.handler *> RecordHandler(_ => clock.sleep(1.second))
      handleResult <- ParallelRecordHandler2.make(slowHandler, partitions).use { handler =>
        val handleRecords = ZIO.foreach_(0 until partitions) { partition =>
          handler.handle(Record(topic, partition, 0L, Headers.Empty, None, s"message-$partition"))
        }
        val collectResults = sink.messages.run(collectAllN[Record[String, String]](partitions))
        handleRecords *> collectResults
      }.timed
      (handleTime, records) = handleResult
    } yield (handleTime must beLessThan(2.seconds)) and (records.distinct must haveSize(partitions))
  }

}

case class Foo(foo: String)

case class Bar(bar: String)
