package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Headers.Header
import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import zio.Task

trait ContextEncoder[-C] {
  def encode[K, V](record: ProducerRecord[K, V], context: C): Task[ProducerRecord[K, V]]
}

object ContextEncoder {
  def aHeaderContextEncoder[C](header: Header, serializer: Serializer[C]): ContextEncoder[C] =
    new ContextEncoder[C] {
      override def encode[K, V](record: ProducerRecord[K, V], context: C): Task[ProducerRecord[K, V]] =
        serializer.serialize(record.topic, context).map { serialized =>
          record.copy(headers = record.headers + (header -> serialized))
        }
    }
}
