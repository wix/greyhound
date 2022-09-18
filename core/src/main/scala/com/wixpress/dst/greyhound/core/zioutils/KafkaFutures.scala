package com.wixpress.dst.greyhound.core.zioutils

import org.apache.kafka.common.KafkaFuture
import zio.{Task, Trace, ZIO}

object KafkaFutures {
  implicit class KafkaFutureOps[A](val future: KafkaFuture[A]) {
    def asZio(implicit trace: Trace): Task[A] = {
      ZIO.asyncInterrupt[Any, Throwable, A] { cb =>
        future.whenComplete { (a, e) => cb(if (e == null) ZIO.succeed(a) else ZIO.fail(e)) }
        Left(ZIO.attemptBlocking(future.cancel(true)).ignore)
      }
    }
  }
}
