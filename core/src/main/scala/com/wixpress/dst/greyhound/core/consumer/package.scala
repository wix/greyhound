package com.wixpress.dst.greyhound.core

import java.util

import com.wixpress.dst.greyhound.core.consumer.domain.{ConsumerSubscription, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import zio.{RIO, ZIO}
import zio.blocking.Blocking
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import scala.collection.JavaConverters._

package object consumer {
  def subscribe[R](subscription: ConsumerSubscription, listener: RebalanceListener[R])(consumer: Consumer): RIO[Blocking with GreyhoundMetrics with R, Unit] =
    ZIO.whenCase(subscription) {
      case TopicPattern(pattern, _) =>
        consumer.subscribePattern(pattern, listener)
      case Topics(topics) =>
        consumer.subscribe(topics, listener)
    }

  def kafkaOffsets(offsets: Map[TopicPartition, Offset]): util.Map[KafkaTopicPartition, OffsetAndMetadata] =
    offsets.map {
      case (tp, offset) => tp.asKafka -> new OffsetAndMetadata(offset)
    }.asJava

  def kafkaPartitions(partitions: Set[TopicPartition]): util.Collection[KafkaTopicPartition] = {
    partitions.map(_.asKafka).asJavaCollection
  }
}
