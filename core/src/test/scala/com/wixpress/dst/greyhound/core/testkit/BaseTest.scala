package com.wixpress.dst.greyhound.core.testkit

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import org.specs2.execute.{AsResult, Result}
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.specs2.specification.core.{Fragment, Fragments}
import zio.{managed, _}
import zio.managed._
import zio.test.{TestClock, TestEnvironment}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.TimeoutException

abstract class BaseTest[R /*: EnvironmentTag*/] extends SpecificationWithJUnit with TimeoutSupport {
  implicit val trace = Trace.empty
  type ENV = R

  val runtime = zio.Runtime.default

//  final val environmentTag: EnvironmentTag[R] = EnvironmentTag[R]

  def env: UManaged[ZEnvironment[R]]

  def run[R1 >: R, E, A](zio: ZIO[R1, E, A]): A =
    Unsafe.unsafe { implicit s => runtime.unsafe.run(env.use(e => zio.provideEnvironment(e))).getOrThrowFiberFailure() }

  def allPar[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*): ZIO[R1, E, Fragments] =
    ZIO.collectAllPar(fragments).map(fragments => Fragments(fragments: _*))

  def all[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*): ZIO[R1, E, Fragments] =
    ZIO.collectAll(fragments).map(fragments => Fragments(fragments: _*))

  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] =
    new AsResult[ZIO[R1, E, A]] {
      override def asResult(t: => ZIO[R1, E, A]): Result = {
        ev.asResult(run(t))
      }
  }

  def testEnvironment: UManaged[ZEnvironment[TestEnvironment]] =
    BaseTestEnvHelper.testEnvironment

  def testClock = BaseTestEnvHelper.testClock

  def anyEnv = BaseTestEnvHelper.anyEnv
}

trait BaseTestNoEnv extends BaseTest[Any] {
  override def env  = managed.ZManaged.succeed(ZEnvironment())(zio.Trace.empty)
}

abstract class BaseTestWithSharedEnv[R, SHARED] extends SpecificationWithJUnit with BeforeAfterAll with TimeoutSupport {
  implicit val trace = Trace.empty

  protected def reportInterruptedFibers: Boolean = false

  val runtime = zio.Runtime.default

  private val sharedRef = new AtomicReference[Option[(SHARED, Task[Unit])]](None)

  def sharedEnv: ZManaged[R, Throwable, SHARED]

  def env: UManaged[ZEnvironment[R]]


  override def beforeAll(): Unit = {
    sharedRef.set(Some(initShared()))
  }

  private def initShared(): (SHARED, UIO[Unit]) = {
    zio.Unsafe.unsafe { implicit s =>
      runtime.unsafe.run(
        env.use(e =>
          ZIO.debug(s"***** Shared environment initializing ****") *>
            (for {
              reservation <- sharedEnv.reserve
              acquired <- reservation.acquire
                .provideEnvironment(e)
                .timed
                .tapBoth(
                  (error: Throwable) =>
                    ZIO.succeed(
                      new Throwable(
                        s"***** shared environment initialization failed - ${error.getClass.getName}: ${error.getMessage}",
                        error
                      )
                        .printStackTrace()
                    ),
                  { case (elapsed, _) => ZIO.debug(s"***** Shared environment initialized in ${elapsed.toMillis} ms *****") }
                )
              (_, res) = acquired
            } yield res -> reservation.release(Exit.unit).unit.provideEnvironment(e))
        )
      ).getOrThrowFiberFailure()
    }
  }

  override def afterAll(): Unit = {
    sharedRef.get.foreach {
      case (_, release) =>
        zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(release).getOrThrowFiberFailure() }
    }
  }

  def getShared: Task[SHARED] =
    ZIO
      .fromOption(sharedRef.get.map(_._1))
      .orElseFail(new RuntimeException("shared environment not initialized"))

  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] =
    new AsResult[ZIO[R1, E, A]] {
      override def asResult(t: => ZIO[R1, E, A]): Result = {
        ev.asResult(zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(
          env.use(e => t.provideEnvironment(e))).getOrThrowFiberFailure() })
      }
    }
}

case class TestAndGreyhoundMetrics(g: GreyhoundMetrics, t: TestMetrics)

trait TimeoutSupport {
  private implicit val trace =  Trace.empty

  implicit class ZioOps[R <: Any : Tag, E, A](zio: ZIO[R, E, A]) {
    def withTimeout(duration: Duration, message: Option[String] = None) (implicit trace: Trace): ZIO[R, Any, A] = {
      val msg = message.getOrElse(s"Timed out after ${duration.render}")
      zio
        .timeoutFail(new TimeoutException(msg))(duration)
    }
  }
}

object BaseTestEnvHelper {
  val testEnvironment: UManaged[ZEnvironment[TestEnvironment]] =
    ZManaged.fromZIO(TestEnvironment.live.build.provide(zio.test.liveEnvironment, ZLayer.succeed(Scope.global)))

  val testClock =
    ZManaged.fromZIO(
      TestClock.default.build
        .provide(zio.test.Annotations.live, zio.test.Live.default, zio.test.liveEnvironment, ZLayer.succeed(Scope.global))
    )

  val anyEnv = ZManaged.succeed(ZEnvironment.empty)
}

//package com.wixpress.dst.greyhound.core.testkit
//
//import org.specs2.execute.{AsResult, Result}
//import org.specs2.mutable.SpecificationWithJUnit
//import org.specs2.specification.BeforeAfterAll
//import org.specs2.specification.core.{Fragment, Fragments}
//import zio.Console.printLine
//import zio._
//import zio.managed._
//
//import java.util.concurrent.atomic.AtomicReference
//import scala.concurrent.TimeoutException
//
//abstract class BaseTest[R : Tag] extends SpecificationWithJUnit with TimeoutSupport {
//  type ENV = R
//
//  implicit val trace = Trace.empty
//
//  val runtime = RuntimeHelper.runtimeWithR(env)
//
//  def env: UManaged[R]
//
//  def run[R1 >: R, E, A](io: ZIO[R1, E, A])(implicit tag: Tag[R]): A = {
//    implicit val trace = Trace.empty
//    zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(io).getOrThrowFiberFailure() }
//  }
//
//  def allPar[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*)(implicit trace: Trace): ZIO[R1, E, Fragments] =
//    ZIO.collectAllPar(fragments).map(fragments => Fragments(fragments: _*))
//
//  def all[R1 >: R, E](fragments: ZIO[R1, E, Fragment]*)(implicit trace: Trace): ZIO[R1, E, Fragments] =
//    ZIO.collectAll(fragments).map(fragments => Fragments(fragments: _*))
//
//  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A], trace: Trace, tag: Tag[R]): AsResult[ZIO[R1, E, A]] =
//    new AsResult[ZIO[R1, E, A]] {
//      override def asResult(t: => ZIO[R1, E, A]): Result = {
//        ev.asResult(zio.Unsafe.unsafe { implicit s =>
//          runtime.unsafe.run(t).getOrThrowFiberFailure() })
//      }
//    }
//}
//
//// remove this and BaseTest, move the tests to zio test, use shared environment
//abstract class BaseTestWithSharedEnv[R <: Any : Tag, SHARED] extends SpecificationWithJUnit with BeforeAfterAll with TimeoutSupport {
//  private implicit val trace = Trace.empty
//
//  val runtime = RuntimeHelper.runtimeWithR(env)
//
//  private val sharedRef = new AtomicReference[Option[(SHARED, RIO[R, Unit])]](None)
//
//  def sharedEnv: ZManaged[R, Throwable, SHARED]
//
//  def env: UManaged[R]
//
//  override def beforeAll(): Unit = {
//    implicit val trace = Trace.empty
//    sharedRef.set(Some(initShared()))
//  }
//
//  private def initShared()(implicit trace: Trace): (SHARED, RIO[R, Unit]) = {
//    zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(
//        printLine(s"***** Shared environment initializing ****") *>
//          (for {
//            reservation <- sharedEnv.reserve
//            acquired    <- reservation.acquire
//              .timed
//              .tapBoth(
//                (error: Throwable) =>
//                  ZIO.succeed(
//                    new Throwable(
//                      s"***** shared environment initialization failed - ${error.getClass.getName}: ${error.getMessage}",
//                      error
//                    )
//                      .printStackTrace()
//                  ),
//                { case (elapsed, _) => printLine(s"***** Shared environment initialized in ${elapsed.toMillis} ms *****") }
//              )
//            (_, res)     = acquired
//          } yield res -> reservation.release(Exit.unit).unit)
//    ).getOrThrowFiberFailure() }
//  }
//
//  override def afterAll(): Unit = {
//    implicit val trace = Trace.empty
//    sharedRef.get.foreach {
//      case (_, release) =>
//        zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(release).getOrThrowFiberFailure() }
//    }
//  }
//
//  def getShared (implicit trace: Trace): Task[SHARED] = {
//    ZIO
//      .fromOption(sharedRef.get.map(_._1))
//      .orElseFail(new RuntimeException("shared environment not initialized"))
//  }
//
//  implicit def zioAsResult[R1 >: R, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R1, E, A]] = {
//    implicit val trace = Trace.empty
//    new AsResult[ZIO[R1, E, A]] {
//      override def asResult(t: => ZIO[R1, E, A]): Result = {
//        ev.asResult(
//          zio.Unsafe.unsafe { implicit s => runtime.unsafe.run(t).getOrThrowFiberFailure() }
//        )
//      }
//    }
//  }
//}
//
//trait TimeoutSupport {
//  implicit class ZioOps[R <: Any: Tag, E, A](zio: ZIO[R, E, A]) {
//    def withTimeout(duration: Duration, message: Option[String] = None)(implicit trace: Trace): ZIO[R, Any, A] = {
//      val msg = message.getOrElse(s"Timed out after ${duration.render}")
//      zio.disconnect
//        .timeoutFail(new TimeoutException(msg))(duration)
////        .tapError(_ => FiberTracking.fromRuntime.flatMap(fs => Fiber.putDumpStr(s"$msg, dumping fibers:\n", fs: _*)))
////        .provideSome[R](_.union[Console](zenv))
//    }
//  }
//}
//
//object RuntimeHelper {
//  def runtimeWithR[R : Tag](env: UManaged[R]) = {
//    implicit val trace   = Trace.empty
//
//    val r = zio.Unsafe.unsafe { implicit s =>
//      zio.Runtime.default.unsafe.run {
//        //we want to try and get the scope and obtain the close function, like the managed hack we did before
//        // together with providing global scope?? i don't think it's possible
////        val closeTask = ZIO.environment[Scope.Closeable].map(_.get.close(Exit.unit))
//        env.reserve.
//      }.getOrThrowFiberFailure()
//    }
//
//    zio.Runtime.default.withEnvironment[R](ZEnvironment(r))
//  }
//
//}