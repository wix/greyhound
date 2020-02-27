package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Deserializer
import com.wixpress.dst.greyhound.core.Headers.Header
import com.wixpress.dst.greyhound.core.consumer.ConsumerRecord
import zio.Task

trait ContextDecoder[+C] {
  def decode[K, V](record: ConsumerRecord[K, V]): Task[C]
}

object ContextDecoder {
  def aHeaderContextDecoder[C](header: Header,
                               deserializer: Deserializer[C],
                               default: C): ContextDecoder[C] =
    new ContextDecoder[C] {
      override def decode[K, V](record: ConsumerRecord[K, V]): Task[C] =
        record.headers.get(header, deserializer).map(_.getOrElse(default))
    }
}
