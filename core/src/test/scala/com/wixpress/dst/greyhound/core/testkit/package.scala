package com.wixpress.dst.greyhound.core

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import zio.Schedule._
import zio.{Chunk, Has, RIO, Ref, Schedule, UIO, ZIO}
import zio.clock.Clock
import zio.duration._

package object testkit {
  type TestMetrics = Has[TestMetrics.Service] with GreyhoundMetrics

  def eventuallyZ[R <: Has[_], T](f: RIO[R, T], timeout: Duration = 4.seconds)(predicate: T => Boolean): ZIO[R, Throwable, Unit] =
    eventuallyTimeoutFail(f)(predicate)(timeout)

  def eventuallyNotZ[R <: Has[_], T](f: RIO[R, T], timeout: Duration = 4.seconds)(predicate: T => Boolean): ZIO[R, Throwable, Unit] =
    eventuallyNotTimeoutFail(f)(predicate)(timeout)

  def eventuallyTimeoutFail[R <: Has[_], T](f: RIO[R, T])(predicate: T => Boolean)(timeout: Duration): ZIO[R, Throwable, Unit] =
    for {
      timeoutRes <- eventuallyTimeout(f)(predicate)(timeout)
      result      = timeoutRes.map(_._2)
      _          <- ZIO.when(timeoutRes.isEmpty)(
                      ZIO.fail(new RuntimeException(s"eventuallyZ predicate failed after ${timeout.toMillis} milliseconds. result: $result"))
                    )
    } yield ()

  def eventuallyNotTimeoutFail[R <: Has[_], T](f: RIO[R, T])(predicate: T => Boolean)(timeout: Duration): ZIO[R, Throwable, Unit] =
    for {
      timeoutRes <- eventuallyTimeout(f)(predicate)(timeout)
      result      = timeoutRes.map(_._2)
      _          <- ZIO.when(timeoutRes.nonEmpty)(
                      ZIO.fail(new RuntimeException(s"eventuallyZ predicate failed after ${timeout.toMillis} milliseconds. result: $result"))
                    )
    } yield ()

  def eventuallyTimeout[R <: Has[_], T](f: RIO[R, T])(predicate: T => Boolean)(timeout: Duration): ZIO[R, Throwable, Option[(Long, T)]] =
    for {
      resultRef  <- Ref.make[Option[T]](None)
      timeoutRes <- f
                      .flatMap(r => resultRef.set(Some(r)) *> UIO(r))
                      .repeat(spaced(100.millis) && Schedule.recurUntil(predicate))
                      .timeout(timeout)
                      .provideSomeLayer[R](Clock.live)
    } yield timeoutRes

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
