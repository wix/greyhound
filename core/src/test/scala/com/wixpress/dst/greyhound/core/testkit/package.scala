package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import zio.Schedule._

import zio._

package object testkit {
  implicit val trace = Trace.empty
  type TestMetrics = TestMetrics.Service with GreyhoundMetrics.Service
  val TestMetricsEnvironment = ZEnvironment(GreyhoundMetrics.live) ++
    zio.Unsafe.unsafe { implicit s => zio.Runtime.default.unsafe.run(TestMetrics.makeEnv()).getOrThrowFiberFailure() }

  def eventuallyZ[R <: Any, T](f: RIO[R, T], timeout: Duration = 4.seconds, message: String = "")(
    predicate: T => Boolean
  ): ZIO[R, Throwable, Unit] =
    eventuallyTimeoutFail(f)(predicate)(timeout, if (message.isBlank) None else Some(message))

  def eventuallyNotZ[R <: Any, T](f: RIO[R, T], timeout: Duration = 4.seconds)(predicate: T => Boolean): ZIO[R, Throwable, Unit] =
    eventuallyNotTimeoutFail(f)(predicate)(timeout)

  def eventuallyTimeoutFail[R <: Any, T](
    f: RIO[R, T]
  )(predicate: T => Boolean)(timeout: Duration, message: Option[String] = None): ZIO[R, Throwable, Unit] = {
    implicit val trace = Trace.empty
    for {
      timeoutRes <- eventuallyTimeout(f)(predicate)(timeout)
      result      = timeoutRes.map(_._2)
      _          <-
        ZIO.when(timeoutRes.isEmpty)(
          ZIO.fail(
            new RuntimeException(
              s"eventuallyZ predicate failed after ${timeout.toMillis} milliseconds${message.map(m => s" ($m)").getOrElse("")}. result: $result"
            )
          )
        )
    } yield ()
  }

  def eventuallyNotTimeoutFail[R <: Any, T](f: RIO[R, T])(predicate: T => Boolean)(timeout: Duration): ZIO[R, Throwable, Unit] = {
    implicit val trace = Trace.empty
    for {
      timeoutRes <- eventuallyTimeout(f)(predicate)(timeout)
      result      = timeoutRes.map(_._2)
      _          <- ZIO.when(timeoutRes.nonEmpty)(
                      ZIO.fail(new RuntimeException(s"eventuallyZ predicate failed after ${timeout.toMillis} milliseconds. result: $result"))
                    )
    } yield ()
  }

  def eventuallyTimeout[R <: Any, T](f: RIO[R, T])(predicate: T => Boolean)(timeout: Duration): ZIO[R, Throwable, Option[(Long, T)]] = {
    implicit val trace = Trace.empty
    for {
      resultRef  <- Ref.make[Option[T]](None)
      timeoutRes <- f
                      .flatMap(r => resultRef.set(Some(r)) *> ZIO.succeed(r))
                      .repeat(spaced(100.millis) && Schedule.recurUntil(predicate))
                      .timeout(timeout)
    } yield timeoutRes
  }

  implicit class StringOps(val str: String)                                              {
    def bytesChunk = Chunk.fromArray(str.getBytes("UTF8"))
  }
  implicit class ByteChunkOps(val chunk: Chunk[Byte])                                    {
    def asString = new String(chunk.toArray, "UTF8")
  }
  implicit class ProducerRecordOps(val record: ProducerRecord[Chunk[Byte], Chunk[Byte]]) {
    def valueString            = record.value.fold("")(_.asString)
    def headerStr(key: String) = record.headers.headers.get(key).fold("")(_.asString)
  }
}
