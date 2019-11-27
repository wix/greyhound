package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Topic
import com.wixpress.dst.greyhound.core.consumer.Consumer.{Key, Value}
import com.wixpress.dst.greyhound.core.serialization.Deserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import zio.{Task, URIO, ZIO}

trait ConsumerSpec {
  def topic: String // TODO LIST TOPICS
  def group: String
  def handler: RecordHandler[Any, Nothing, Key, Value]
}

object ConsumerSpec {

  def make[R, K, V](topic: Topic[K, V],
                    group: String,
                    handler: RecordHandler[R, Nothing, K, V],
                    keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V]): URIO[R, ConsumerSpec] =
    ZIO.access[R] { r =>
      val topicName = topic.name
      val group0 = group
      val handler0 = handler
        .contramapM(deserialize(keyDeserializer, valueDeserializer))
        .withErrorHandler(e => ZIO.unit) // TODO report serialization errors
        .provide(r)

      new ConsumerSpec {
        override val topic: String = topicName
        override val group: String = group0
        override val handler = handler0
      }
    }

  def deserialize[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V])
                       (record: Consumer.Record): Task[ConsumerRecord[K, V]] = for {
    key <- keyDeserializer.deserialize(record.topic, record.headers, record.key)
    value <- valueDeserializer.deserialize(record.topic, record.headers, record.value)
  } yield new ConsumerRecord[K, V](
    record.topic,
    record.partition,
    record.offset,
    record.timestamp,
    record.timestampType,
    ConsumerRecord.NULL_CHECKSUM.toLong, // Checksum is deprecated
    record.serializedKeySize,
    record.serializedValueSize,
    key,
    value,
    record.headers)

}
