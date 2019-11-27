package com.wixpress.dst.greyhound.testkit

import com.wixpress.dst.greyhound.core.consumer.RecordHandler
import zio.stream.{Sink, Stream}
import zio.{Queue, UIO, ZIO}

trait MessagesSink[K, V] {
  def handler: RecordHandler[Any, Nothing, K, V]

  def messages: Stream[Nothing, (K, V)]

  def firstMessage: ZIO[Any, Unit, (K, V)] =
    messages.run(Sink.await[(K, V)])
}

object MessagesSink {
  def make[K, V](capacity: Int = 16): UIO[MessagesSink[K, V]] =
    Queue.bounded[(K, V)](capacity).map { queue =>
      new MessagesSink[K, V] {
        override def messages: Stream[Nothing, (K, V)] =
          Stream.fromQueueWithShutdown(queue)

        override def handler: RecordHandler[Any, Nothing, K, V] =
          RecordHandler(record => queue.offer(record.key -> record.value))
      }
    }
}
