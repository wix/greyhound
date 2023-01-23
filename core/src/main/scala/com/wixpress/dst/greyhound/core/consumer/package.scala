package com.wixpress.dst.greyhound.core

import java.util
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerSubscription.{TopicPattern, Topics}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import org.apache.kafka.clients.consumer.{OffsetAndMetadata => KafkaOffsetAndMetadata}
import zio.{RIO, Tag, Trace, ZIO}
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}

import scala.collection.JavaConverters._

package object consumer {
  def subscribe[R](subscription: ConsumerSubscription, listener: RebalanceListener[R])(
    consumer: Consumer
  )(implicit trace: Trace): RIO[GreyhoundMetrics with R, Unit] =
    ZIO
      .whenCase(subscription) {
        case TopicPattern(pattern, _) =>
          consumer.subscribePattern(pattern, listener)
        case Topics(topics)           =>
          consumer.subscribe(topics, listener)
      }
      .unit

  def kafkaOffsets(offsets: Map[TopicPartition, Offset]): util.Map[KafkaTopicPartition, KafkaOffsetAndMetadata] =
    offsets.map { case (tp, offset) => tp.asKafka -> new KafkaOffsetAndMetadata(offset) }.asJava

  def kafkaOffsetsAndMetaData(offsets: Map[TopicPartition, OffsetAndMetadata]): util.Map[KafkaTopicPartition, KafkaOffsetAndMetadata] =
    offsets.map { case (tp, om) => tp.asKafka -> new KafkaOffsetAndMetadata(om.offset, om.metadata) }.asJava

  def toOffsetsAndMetadata(
    offsets: Map[TopicPartition, Offset],
    metadataString: Metadata = OffsetAndMetadata.NO_METADATA
  ): Map[TopicPartition, OffsetAndMetadata] =
    offsets.map { case (tp, offset) => tp -> OffsetAndMetadata(offset, metadataString) }

  def kafkaPartitions(partitions: Set[TopicPartition]): util.Set[KafkaTopicPartition] = {
    partitions.map(_.asKafka).asJava
  }

}
