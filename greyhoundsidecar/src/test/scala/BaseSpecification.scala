
import org.specs2.execute.{AsResult, Error, Result}
import org.specs2.mutable.Specification
import zio.{BootstrapRuntime, FiberFailure, ZEnv, ZIO}

trait ZioScope {
  def zioTest: ZIO[_, _, _]
}

trait BaseSpecification extends Specification with BootstrapRuntime {

  implicit def zioAsResult[R >: ZEnv, E, A](implicit ev: AsResult[A]): AsResult[ZIO[R, E, A]] =
    new AsResult[ZIO[R, E, A]] {
      override def asResult(zio: => ZIO[R, E, A]): Result = {
        unsafeRunSync(zio).fold(
          e => Error(FiberFailure(e)),
          a => ev.asResult(a))
      }
    }

  implicit def zioScopeAsResult[Z <: ZioScope, R >: ZEnv, E, A]: AsResult[Z] =
    zioScope => {
      val zio = zioScope.zioTest.asInstanceOf[ZIO[R, E, A]]

      unsafeRunSync(zio).fold(
        e => Error(FiberFailure(e)),
        a => Result.resultOrSuccess(a))
    }

}
