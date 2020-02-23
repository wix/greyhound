package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core.Serializer
import com.wixpress.dst.greyhound.core.producer.{ProducerRecord, RecordMetadata}

import scala.concurrent.Future

trait GreyhoundProducer {
  def produce[K, V](record: ProducerRecord[K, V],
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): Future[RecordMetadata]
}
