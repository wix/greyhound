package com.wixpress.dst.greyhound.core.combined

import com.wixpress.dst.greyhound.core.ConsumerIT
import com.wixpress.dst.greyhound.core.ConsumerIT._
import com.wixpress.dst.greyhound.core.Serdes._
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.BaseTest
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers.{beRecordWithKey, beRecordWithValue}
import com.wixpress.dst.greyhound.testkit.ManagedKafka
import zio._

class CombinedIT extends BaseTest[Env] {
  sequential

  override def env: UManaged[Env] = ConsumerIT.ManagedEnv

  val resources = testResources()

  val tests = resources.use {
    case (kafka, producer) =>
      implicit val _kafka: ManagedKafka = kafka

      val combinedHandlersTest = for {
        _ <- console.putStrLn(">>>> starting test: combinedHandlersTest")

        topic1 <- createRandomTopic()
        topic2 <- createRandomTopic()
        group <- randomGroup

        records1 <- Queue.unbounded[ConsumerRecord[String, String]]
        records2 <- Queue.unbounded[ConsumerRecord[Int, Int]]
        handler1 = RecordHandler(topic1)(records1.offer(_: ConsumerRecord[String, String]))
        handler2 = RecordHandler(topic2)(records2.offer(_: ConsumerRecord[Int, Int]))
        handler = handler1.withDeserializers(StringSerde, StringSerde) combine handler2.withDeserializers(IntSerde, IntSerde)

        (record1, record2) <- RecordConsumer.make(RecordConsumerConfig(kafka.bootstrapServers, group), handler.ignore).use_ {
          producer.produce(ProducerRecord(topic1, "bar", Some("foo")), StringSerde, StringSerde) *>
            producer.produce(ProducerRecord(topic2, 2, Some(1)), IntSerde, IntSerde) *>
            (records1.take zip records2.take)
        }
      } yield "consume messages from combined handlers" in {
        (record1 must (beRecordWithKey("foo") and beRecordWithValue("bar"))) and
          (record2 must (beRecordWithKey(1) and beRecordWithValue(2)))
      }


      all(combinedHandlersTest)
  }

  run(tests)

}

