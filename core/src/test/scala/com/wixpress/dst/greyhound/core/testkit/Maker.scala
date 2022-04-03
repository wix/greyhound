package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.Headers
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import zio.random.{nextBytes, nextIntBounded}
import zio.{random, ZIO}

object Maker {
  val bytes = nextIntBounded(9).flatMap(size => nextBytes(size + 1))

  def randomAlphaChar = {
    val low  = 'A'.toInt
    val high = 'z'.toInt + 1
    random.nextIntBetween(low, high).map(_.toChar)
  }

  def randomStr = ZIO.collectAll(List.fill(6)(randomAlphaChar)).map(_.mkString)

  def randomTopicName = randomStr.map(suffix => s"some-topic-$suffix")

  val partition = 0
  val offset    = 0
  val key       = s"some-key-$randomStr"
  val value     = randomStr

  val abytesRecord = for {
    topic <- randomTopicName
    key   <- bytes
    value <- bytes
  } yield new ConsumerRecord(topic, partition, offset, Headers.Empty, Some(key), value, 0L, 0L, 0L)
}
