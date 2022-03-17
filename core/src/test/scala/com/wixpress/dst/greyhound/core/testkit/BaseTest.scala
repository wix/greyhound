package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.zioutils.FiberTrackingMode.Tracking
import org.specs2.execute.{AsResult, Error, Result}
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.specs2.specification.core.{Fragment, Fragments}
import zio.{BootstrapRuntime, UManaged}
import zio._
import zio.console.{Console, putStrLn}
import zio.internal.Platform
import com.wixpress.dst.greyhound.core.zioutils._
import zio.clock.Clock
import zio.duration._

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.TimeoutException

trait BaseTest[R]
  extends SpecificationWithJUnit with TimeoutSupport{

  type ENV = R

  val runtime = new BootstrapRuntime{}.withFiberTracking(Tracking())

  def env: UManaged[R]

  def run[R1 >: R, E, A](zio: ZIO[R1, E, A]): A =
    runtime.unsafeRun(env.use(zio.provide))

  def allPar[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*): ZIO[R1, E, Fragments] =
    ZIO.collectAllPar(fragments).map(fragments => Fragments(fragments: _*))

  def all[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*): ZIO[R1, E, Fragments] =
    ZIO.collectAll(fragments).map(fragments => Fragments(fragments: _*))

  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] =
    new AsResult[ZIO[R1, E, A]] {
      override def asResult(t: => ZIO[R1, E, A]): Result =
        runtime.unsafeRunSync(env.use(t.provide)).fold(
          e => Error(e.squashTraceWith {
            case ex: Throwable => ex
            case _ => FiberFailure(e): Throwable
          }),
          a => ev.asResult(a))
    }
}

trait BaseTestWithSharedEnv[R <: Has[_], SHARED] extends SpecificationWithJUnit with BeforeAfterAll with TimeoutSupport {

  protected def reportInterruptedFibers: Boolean = false

  val runtime = new BootstrapRuntime {
    override val platform: Platform = Platform.default.withReportFailure { cause =>
      if (cause.interrupted) {
        if(reportInterruptedFibers)
          println(s"Fiber interrupted:\n${ cause.prettyPrint }")
      } else {
        scala.Console.err.println(s"Unhandled failure:\n${ cause.prettyPrint }")
      }
    }
  }.withFiberTracking(Tracking())

  private val sharedRef = new AtomicReference[Option[(SHARED, Task[Unit])]](None)

  def sharedEnv: ZManaged[R, Throwable, SHARED]

  def env: UManaged[R]

  override def beforeAll(): Unit = {
    sharedRef.set(Some(initShared()))
  }

  private def initShared(): (SHARED, UIO[Unit]) = {
    runtime.unsafeRunTask(env.use(e =>
      putStrLn(s"***** Shared environment initializing ****") *>
        (for {
          reservation <- sharedEnv.reserve
          acquired <- reservation.acquire.provide(e).timed.tapBoth (
            (error:Throwable) => UIO(new Throwable(s"***** shared environment initialization failed - ${error.getClass.getName}: ${error.getMessage}", error).printStackTrace()),
            { case (elapsed, _) => putStrLn(s"***** Shared environment initialized in ${elapsed.toMillis} ms *****") }
          )
          (_, res)  = acquired
        } yield res -> reservation.release(Exit.unit).unit.provide(e))
    ))
  }

  override def afterAll(): Unit = {
    sharedRef.get.foreach {
      case (_, release) =>
        runtime.unsafeRunTask(release)
    }
  }

  def getShared: Task[SHARED] = {
    ZIO.fromOption(sharedRef.get.map(_._1))
      .orElseFail(new RuntimeException("shared environment not initialized"))
  }

  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] =
    new AsResult[ZIO[R1, E, A]] {
      override def asResult(t: => ZIO[R1, E, A]): Result =
        runtime.unsafeRunSync(env.use(t.provide)).fold(
          e => Error(e.squashTraceWith {
            case ex: Throwable => ex
            case _ => FiberFailure(e): Throwable
          }),
          a => ev.asResult(a))
    }
}

trait TimeoutSupport {
  private val zenv: Clock with Console = Has(Clock.Service.live) ++ Has(Console.Service.live)
  implicit class ZioOps[R <: Has[_] : Tag, E, A](zio: ZIO[R, E, A]) {
    def withTimeout(duration: Duration, message: Option[String] = None): ZIO[R, Any, A] = {
      val msg = message.getOrElse(s"Timed out after ${duration.render}")
      zio.disconnect.timeoutFail(new TimeoutException(msg))(duration)
        .tapError(_ => FiberTracking.fromRuntime.flatMap(fs => Fiber.putDumpStr(s"$msg, dumping fibers:\n", fs:_*)))
        .provideSome[R](_.union[Clock with Console](zenv))
    }
  }
}