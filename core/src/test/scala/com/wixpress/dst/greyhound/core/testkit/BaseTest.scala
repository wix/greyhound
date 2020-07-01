package com.wixpress.dst.greyhound.core.testkit

import java.util.concurrent.atomic.AtomicReference

import com.wixpress.dst.greyhound.core.zioutils.ZManagedSyntax._
import org.specs2.execute.{AsResult, Error, Result}
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.specs2.specification.core.{Fragment, Fragments}
import zio.console.putStrLn
import zio.internal.Platform
import zio.{Reservation => _, _}

trait BaseTest[R]
  extends SpecificationWithJUnit
    with BootstrapRuntime {

  def env: UManaged[R]

  def run[R1 >: R, E, A](zio: ZIO[R1, E, A]): A =
    unsafeRun(env.use(zio.provide))

  def allPar[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*): ZIO[R1, E, Fragments] =
    ZIO.collectAllPar(fragments).map(fragments => Fragments(fragments: _*))

  def all[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*): ZIO[R1, E, Fragments] =
    ZIO.collectAll(fragments).map(fragments => Fragments(fragments: _*))

  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] =
    new AsResult[ZIO[R1, E, A]] {
      override def asResult(t: => ZIO[R1, E, A]): Result =
        unsafeRunSync(env.use(t.provide)).fold(
          e => Error(e.squashTraceWith {
            case ex: Throwable => ex
            case _ => FiberFailure(e): Throwable
          }),
          a => ev.asResult(a))
    }

}

trait BaseTestWithSharedEnv[R <: Has[_], SHARED] extends SpecificationWithJUnit with BeforeAfterAll {

  protected def reportInterruptedFibers: Boolean = false;

  val runtime = new BootstrapRuntime {
    override val platform: Platform = Platform.default.withReportFailure { cause =>
      if (cause.interrupted) {
        if(reportInterruptedFibers)
          println(s"Fiber interrupted:\n${ cause.prettyPrint }")
      } else {
        scala.Console.err.println(s"Unhandled failure:\n${ cause.prettyPrint }")
      }
    }
  }

  private val sharedRef = new AtomicReference[Option[Reservation[SHARED]]](None)

  def sharedEnv: ZManaged[R, Throwable, SHARED]

  def env: UManaged[R]

  override def beforeAll(): Unit = {
    sharedRef.set(Some(initShared()))
  }

  private def initShared() = {
    runtime.unsafeRunTask(env.use(r =>
      putStrLn(s"***** Shared environment initializing ****") *>
        sharedEnv.reserve.provide(r).timed.tapBoth (
          error => UIO(new Throwable(s"***** shared environment initialization failed - ${error.getClass.getName}: ${error.getMessage}", error).printStackTrace()),
          { case (elapsed, _) => putStrLn(s"***** Shared environment initialized in ${elapsed.toMillis} ms *****") }
        ).map(_._2)
    ))
  }

  override def afterAll(): Unit = {
    sharedRef.get.foreach(
      reservation =>
        runtime.unsafeRunTask(reservation.release(Exit.unit))
    )
  }

  def getShared(implicit ev: zio.Tag[SHARED]) :URIO[Has[SHARED], SHARED] = ZIO.access[Has[SHARED]](_.get[SHARED])

  implicit def zioAsResult[R1 >: R with Has[SHARED] : zio.Tag, E, A]
          (implicit ev: AsResult[A], ev3: zio.Tag[SHARED]): AsResult[ZIO[R1, E, A]] = new AsResult[ZIO[R1, E, A]] {
    override def asResult(t: => ZIO[R1, E, A]): Result = {
      runtime.unsafeRunSync(
        env.use { e: R =>
          val sharedEnv: SHARED = sharedRef.get
            .getOrElse(throw new RuntimeException("shared environment not initialized"))
            .acquired
          t.provide(e.++[Has[SHARED]](Has(sharedEnv)))
        }).fold(
        e => Error(e.squashTraceWith{
          case ex: Throwable => ex
          case _ => FiberFailure(e): Throwable
        }),
        a => ev.asResult(a))
    }
  }
}
