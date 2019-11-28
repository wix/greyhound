package com.wixpress.dst.greyhound.core.consumer

import com.wixpress.dst.greyhound.core.Record
import com.wixpress.dst.greyhound.core.consumer.Consumer.{Key, Value}
import zio.{Queue, ZIO, ZManaged}

// TODO rename
object ParallelRecordHandler {
  def apply(specs: Map[String, ConsumerSpec]): RecordHandler[Any, NoHandlerFoundForTopic, Key, Value] =
    RecordHandler { record =>
      specs.get(record.topic) match {
        case Some(spec) => spec.handler.handle(record)
        case None => ZIO.fail(NoHandlerFoundForTopic(record.topic))
      }
    }
}

case class NoHandlerFoundForTopic(topic: String)
  extends RuntimeException(s"No handler found for topic $topic")

object ParallelRecordHandler2 {
  private val capacity = 128

  def make[R, K, V](handler: RecordHandler[R, Nothing, K, V], parallelism: Int): ZManaged[R, Nothing, RecordHandler[Any, Nothing, K, V]] =
    ZManaged.foreach(0 until parallelism)(_ => makeQueue(handler)).map { queues =>
      RecordHandler(record => queues(record.partition % queues.length).offer(record))
    }

  private def makeQueue[R, E, K, V](handler: RecordHandler[R, E, K, V]) = {
    val queue = for {
      queue <- Queue.bounded[Record[K, V]](capacity)
      _ <- queue.take.flatMap(handler.handle).forever.fork
    } yield queue

    queue.toManaged(_.shutdown)
  }
}
