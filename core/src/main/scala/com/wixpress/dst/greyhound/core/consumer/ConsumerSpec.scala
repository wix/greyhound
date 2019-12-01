package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.Consumer.{Key, Value}
import com.wixpress.dst.greyhound.core.consumer.ConsumerSpec.Handler
import com.wixpress.dst.greyhound.core.serialization.Deserializer
import zio.{Task, URIO, ZIO}

trait ConsumerSpec {
  def topic: TopicName // TODO list topics?
  def group: GroupName
  def handler: Handler
  def parallelism: Int
}

object ConsumerSpec {
  type Handler = RecordHandler[Any, Nothing, Key, Value]

  def make[R, K, V](topic: Topic[K, V],
                    group: GroupName,
                    handler: RecordHandler[R, Nothing, K, V],
                    keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    parallelism: Int = 8): URIO[R, ConsumerSpec] =
    ZIO.access[R] { r =>
      val topicName = topic.name
      val group0 = group
      val parallelism0 = parallelism
      val handler0 = handler
        .contramapM(deserialize(keyDeserializer, valueDeserializer))
        .withErrorHandler(e => ZIO.unit) // TODO report serialization errors
        .provide(r)

      new ConsumerSpec {
        override val topic: TopicName = topicName
        override val group: GroupName = group0
        override val handler: Handler = handler0
        override val parallelism: Int = parallelism0
      }
    }

  def deserialize[K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V])
                       (record: Record[Consumer.Key, Consumer.Value]): Task[Record[K, V]] = for {
    key <- record.key.fold[Task[Option[K]]](ZIO.none) { k =>
      keyDeserializer.deserialize(record.topic, record.headers, k).map(Some(_))
    }
    value <- valueDeserializer.deserialize(record.topic, record.headers, record.value)
  } yield Record(
    topic = record.topic,
    partition = record.partition,
    offset = record.offset,
    headers = Headers(),
    key = key,
    value = value)

}
