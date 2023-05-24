package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.Offset
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._

object RecordMatchers {
  def beRecordWithKey[K](key: K): Matcher[ConsumerRecord[K, _]] =
    beSome(key) ^^ ((_: ConsumerRecord[K, _]).key)

  def beRecordWithValue[V](value: V): Matcher[ConsumerRecord[_, V]] =
    equalTo(value) ^^ ((_: ConsumerRecord[_, V]).value)

  def beRecordWithOffset(offset: Offset): Matcher[ConsumerRecord[_, _]] =
    equalTo(offset) ^^ ((_: ConsumerRecord[_, _]).offset)

  def beRecordsWithKeysAndValues[K, V](records: IndexedSeq[ProducerRecord[K, V]]): Matcher[Seq[ConsumerRecord[K, V]]] = {
    val matchers = records.map { r => beRecordWithKey(r.key.get) and beRecordWithValue(r.value.get) }
    allOf(matchers: _*)
  }
}
