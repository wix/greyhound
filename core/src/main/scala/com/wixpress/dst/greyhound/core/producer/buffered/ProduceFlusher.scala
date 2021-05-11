package com.wixpress.dst.greyhound.core.producer.buffered

import java.lang.System.currentTimeMillis

import com.wixpress.dst.greyhound.core.metrics.{GreyhoundMetric, GreyhoundMetrics}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics.report
import com.wixpress.dst.greyhound.core.producer.buffered.Common.{nonRetriable, timeoutPassed}
import com.wixpress.dst.greyhound.core.producer.buffered.LocalBufferProducerMetric.{LocalBufferProduceAttemptFailed, LocalBufferProduceTimeoutExceeded}
import com.wixpress.dst.greyhound.core.producer.buffered.buffers.ProduceStrategy
import com.wixpress.dst.greyhound.core.producer.{ProducerError, ProducerR, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors._
import zio._
import zio.blocking.Blocking
import zio.duration.Duration

import scala.util.Random

trait ProduceFlusher[R] extends ProducerR[R] {
  def recordedConcurrency: UIO[Int]

  def fiberCount: UIO[Int]
}

object ProduceFlusher {
  def make[R](producer: ProducerR[R],
              giveUpAfter: Duration, retryInterval: Duration,
              strategy: ProduceStrategy): URIO[ZEnv with GreyhoundMetrics with R, ProduceFlusher[R]] =
    strategy match {
      case ProduceStrategy.Sync(concurrency) => ProduceFiberSyncRouter.make(producer, concurrency, giveUpAfter, retryInterval)
      case ProduceStrategy.Async(batchSize, concurrency) => ProduceFiberAsyncRouter.make(producer, concurrency, giveUpAfter, retryInterval, batchSize)
      case ProduceStrategy.Unordered(batchSize, concurrency) => ProduceFiberAsyncRouter.make(producer, concurrency, giveUpAfter, retryInterval, batchSize)
    }
}

object ProduceFiberAsyncRouter {
  def make[R](producer: ProducerR[R], maxConcurrency: Int,
              giveUpAfter: Duration, retryInterval: Duration,
              batchSize: Int): URIO[ZEnv with GreyhoundMetrics with R, ProduceFlusher[R]] =
  //also it should be on blocking pool, it can hog fibers..
    for {
      usedFibers <- Ref.make(Set.empty[Int])
      runningFibers <- Ref.make(0)
      queues <- ZIO.foreach((0 until maxConcurrency).toList)(i => Queue.unbounded[ProduceRequest].map(i -> _)).map(_.toMap)
      _ <- ZIO.foreach_(queues.values)(q =>
        fetchAndProduce(producer)(retryInterval, batchSize)(q)
          .forever
          .tapCause(_ => report(LocalBufferProducerInternalFiberDied()) *> runningFibers.update(_ - 1))
          .forkDaemon
          .tap(_ => runningFibers.update(_ + 1))
      )
    } yield new ProduceFlusher[R] {
      override def recordedConcurrency: UIO[Int] = usedFibers.get.map(_.size)

      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[R with Blocking, ProducerError, IO[ProducerError, RecordMetadata]] = {
        val queueNum = Math.abs(record.key.getOrElse(Random.nextString(10)).hashCode % maxConcurrency)

        Promise.make[ProducerError, RecordMetadata].tap(promise =>
          queues(queueNum).offer(ProduceRequest(record, promise, currentTimeMillis + giveUpAfter.toMillis)) *>
            usedFibers.update(_ + queueNum)).map(_.await)
      }

      override def fiberCount: UIO[Int] = runningFibers.get
    }

  private def fetchAndProduce[R](producer: ProducerR[R])(retryInterval: Duration, batchSize: Int) =
    (s: Queue[ProduceRequest]) =>
      s.takeBetween(1, batchSize)
        .flatMap(produceUntilResolution(producer)(level = 0)(retryInterval))

  private def produceUntilResolution[R](producer: ProducerR[R])(level: Int)(retryInterval: Duration)(reqs: Seq[ProduceRequest]): ZIO[GreyhoundMetrics with zio.ZEnv with R with Blocking, ProducerError, Unit] =
    ZIO.when(reqs.nonEmpty)(
      discardOldRequests(reqs) *>
        ZIO.foreach(reqs.filterNot(timeoutPassed)) { req =>
          producer.produceAsync(req.record).map(res => (req, res))
        }.flatMap(awaitOnPromises)
          .flatMap(removeFinalFailures)
          .flatMap(succeedOrRetry(producer, level, retryInterval))
    )

  private def discardOldRequests(reqs: Seq[ProduceRequest]) =
    ZIO.foreach(reqs.filter(timeoutPassed))(reportError)

  private def succeedOrRetry[R](producer: ProducerR[R], level: Int, retryInterval: Duration)(results: Seq[Option[(ProduceRequest, Either[ProducerError, RecordMetadata])]]) = {
    val failures = results.collect { case Some((req, _@Left(_))) => req }
    val successes = results.collect { case Some((req, _@Right(value))) => (req, value) }

    ZIO.foreach_(successes) {
      case (req, res) => req.succeed(res)
    } *>
      ZIO.when(failures.nonEmpty)(
        produceUntilResolution(producer)(level + 1)(retryInterval)(failures).delay(retryInterval))
  }

  private def removeFinalFailures(results: Seq[(ProduceRequest, Either[ProducerError, RecordMetadata])]) =
    ZIO.foreach(results) { case (req, res) =>
      res match {
        case Left(e) if (timeoutPassed(req) || nonRetriable(e.getCause)) =>
          req.fail(e).as(None)
        case l@Left(_) =>
          UIO(Some(req, l))
        case r@Right(_) => UIO(Some(req, r))
      }
    }

  private def awaitOnPromises(promises: Seq[(ProduceRequest, IO[ProducerError, RecordMetadata])]): URIO[GreyhoundMetrics with ZEnv, Seq[(ProduceRequest, Either[ProducerError, RecordMetadata])]] =
    ZIO.foreach(promises) { case (req: ProduceRequest, res: IO[ProducerError, RecordMetadata]) =>
      res.tapError(error => report(LocalBufferProduceAttemptFailed(error, nonRetriable(error.getCause))))
        .either.map(e => (req, e))
    }

  private def reportError(req: ProduceRequest) =
    ProducerError(new TimeoutException).flip.flatMap(timeout =>
      report(LocalBufferProduceTimeoutExceeded(req.giveUpTimestamp, System.currentTimeMillis)) *>
        req.fail(timeout))
}

object ProduceFiberSyncRouter {
  def make[R](producer: ProducerR[R], maxConcurrency: Int, giveUpAfter: Duration, retryInterval: Duration): URIO[ZEnv with GreyhoundMetrics with R, ProduceFlusher[R]] =
    for {
      usedFibers <- Ref.make(Set.empty[Int])
      runningFibers <- Ref.make(0)
      queues <- ZIO.foreach((0 until maxConcurrency).toList)(i => Queue.unbounded[ProduceRequest].map(i -> _)).map(_.toMap)
      _ <- ZIO.foreach_(queues.values)(
        _.take
          .flatMap((req: ProduceRequest) =>
            ZIO.whenCase(timeoutPassed(req)) {
              case true =>
                ProducerError(new TimeoutException).flip.flatMap(timeout =>
                  report(LocalBufferProduceTimeoutExceeded(req.giveUpTimestamp, System.currentTimeMillis)) *>
                    req.fail(timeout))
              case false =>
                producer.produce(req.record)
                  .tapError(error => report(LocalBufferProduceAttemptFailed(error, nonRetriable(error.getCause))))
                  .retry(Schedule.spaced(retryInterval) && Schedule.recurUntil(e => timeoutPassed(req) || nonRetriable(e.getCause)))
                  .tapBoth(req.fail, req.succeed)
            }.ignore
          )
          .forever
          .tapCause(_ => report(LocalBufferProducerInternalFiberDied()) *> runningFibers.update(_ - 1))
          .forkDaemon
          .tap(_ => runningFibers.update(_ + 1)))

    } yield new ProduceFlusher[R] {


      override def fiberCount: UIO[Int] = runningFibers.get

      override def recordedConcurrency: UIO[Int] = usedFibers.get.map(_.size)

      override def produceAsync(record: ProducerRecord[Chunk[Byte], Chunk[Byte]]): ZIO[Blocking, ProducerError, IO[ProducerError, RecordMetadata]] = {
        val queueNum = Math.abs(record.key.getOrElse(Random.nextString(10)).hashCode % maxConcurrency)

        Promise.make[ProducerError, RecordMetadata].tap(promise =>
          queues(queueNum).offer(ProduceRequest(record, promise, currentTimeMillis + giveUpAfter.toMillis)) *>
            usedFibers.update(_ + queueNum)
        ).map(_.await)
      }
    }
}

object Common {
  private[producer] def nonRetriable(e: Throwable): Boolean = e match {
    case _: InvalidTopicException => true
    case _: RecordBatchTooLargeException => true
    case _: UnknownServerException => true
    case _: OffsetMetadataTooLarge => true
    case _: RecordTooLargeException => true
    case _: IllegalArgumentException => true
    case _ => false
  }

  private[producer] def timeoutPassed(req: ProduceRequest): Boolean =
    currentTimeMillis > req.giveUpTimestamp
}

case class LocalBufferProducerInternalFiberDied() extends GreyhoundMetric
