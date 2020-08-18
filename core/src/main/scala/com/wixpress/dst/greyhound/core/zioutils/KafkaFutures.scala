package com.wixpress.dst.greyhound.core.zioutils

import org.apache.kafka.common.KafkaFuture
import zio.blocking.Blocking
import zio.{RIO, ZIO, blocking}

object KafkaFutures {
  implicit class KafkaFutureOps[A](val future: KafkaFuture[A]) {
    def asZio: RIO[Blocking, A] = {
      RIO.effectAsyncInterrupt[Blocking, A] { cb =>
        future.whenComplete{ (a, e) =>
          cb(if(e == null) ZIO.succeed(a) else ZIO.fail(e))
        }
        Left(blocking.effectBlocking(future.cancel(true)).ignore)
      }
    }
  }
}
