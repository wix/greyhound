package com.wixpress.dst.greyhound

import com.wixpress.dst.greyhound.core.Serdes.StringSerde
import zio.Task
import zio.duration.Duration
import java.time.Instant

package object core {
  type ClientId        = String
  type Topic           = String
  type Group           = String
  type Partition       = Int
  type Offset          = Long
  type ConsumerGroupId = Group
  type Metadata        = String

  type NonEmptySet[A]  = Set[A]
  type NonEmptyList[A] = List[A]

  val longDeserializer                           = StringSerde.mapM((str: String) => Task(str.toLong))
  val instantDeserializer: Deserializer[Instant] = longDeserializer.map(Instant.ofEpochMilli)
  val durationDeserializer                       = longDeserializer.map(Duration(_, java.util.concurrent.TimeUnit.MILLISECONDS))

  case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {
    override def toString = s"[$group, ${topicPartition.topic}, ${topicPartition.partition}]"
  }
}
