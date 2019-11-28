package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.Record
import com.wixpress.dst.greyhound.core.consumer.RecordHandler
import zio.stream.{Sink, Stream}
import zio.{Queue, UIO, ZIO}

trait MessagesSink[K, V] {
  def handler: RecordHandler[Any, Nothing, K, V]

  def messages: Stream[Nothing, Record[K, V]]

  def firstMessage: ZIO[Any, Unit, Record[K, V]] =
    messages.run(Sink.await[Record[K, V]])
}

object MessagesSink {
  def make[K, V](capacity: Int = 16): UIO[MessagesSink[K, V]] =
    Queue.bounded[Record[K, V]](capacity).map { queue =>
      new MessagesSink[K, V] {
        override val messages: Stream[Nothing, Record[K, V]] =
          Stream.fromQueueWithShutdown(queue)

        override val handler: RecordHandler[Any, Nothing, K, V] =
          RecordHandler(queue.offer)
      }
    }
}
