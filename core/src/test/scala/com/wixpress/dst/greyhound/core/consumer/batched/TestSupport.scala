package com.wixpress.dst.greyhound.core.consumer.batched

import com.wixpress.dst.greyhound.core.{Headers, TopicPartition}
import com.wixpress.dst.greyhound.core.consumer.Consumer
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import zio.Chunk
import zio.test.Assertion
import zio.test.Assertion.{contains, equalTo, hasSize}

import scala.util.Random

object TestSupport {
  def aRecord(topicPartition: TopicPartition, payload: String = randomStr, hint: String = ""): Consumer.Record = ConsumerRecord(
    topicPartition.topic,
    topicPartition.partition,
    randomPositiveInt,
    Headers.from(Option(hint).filter(_.nonEmpty).map("hint" -> _).toMap),
    Option.empty[Chunk[Byte]],
    Chunk.fromArray(payload.getBytes),
    0L,
    payload.getBytes.length,
    0L
  )

  def records(topicCount: Int = 4, partitions: Int = 4, perPartition: Int = 3, hint: String = "") = {
    val topics = Seq.fill(topicCount)(randomStr)
    val topicPartitions = for {
      topic     <- topics
      partition <- 0 until partitions
    } yield TopicPartition(topic, partition)
    topicPartitions.flatMap(tp => Seq.fill(perPartition)(aRecord(tp, hint)))
  }

  def containsRecordsByPartition(records: Seq[Consumer.Record]) =
    records.groupBy(_.topicPartition).foldLeft[Assertion[Vector[Seq[Consumer.Record]]]](Assertion.anything) {
      case (res, (_, records)) => res && contains(records)
    } && hasSize(equalTo(records.map(_.topicPartition).distinct.size))

  def randomStr: String =
    Random.alphanumeric.take(20).mkString("")

  def randomPositiveInt: Int =
    Random.nextInt(Int.MaxValue)
}
