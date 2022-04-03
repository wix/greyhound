package com.wixpress.dst.greyhound.core

import zio._

package object zioutils {
  type FiberTracking = Has[FiberTracking.Service]

  object FiberTracking {
    trait Service {
      def fibers: UIO[Chunk[Fiber.Runtime[Any, Any]]]
    }

    def fibers: URIO[FiberTracking, Chunk[Fiber.Runtime[Any, Any]]] =
      ZIO.accessM(_.get.fibers)

    def make(supervisor: Supervisor[Chunk[Fiber.Runtime[Any, Any]]]): FiberTracking =
      Has(new Service {
        override def fibers: UIO[Chunk[Fiber.Runtime[Any, Any]]] = supervisor.value
      })

    val NoTracking: FiberTracking = Has(
      new Service {
        override def fibers: UIO[Chunk[Fiber.Runtime[Any, Any]]] = UIO(Chunk.empty)
      }
    )

    def fromRuntime = ZIO
      .runtime[Any]
      .flatMap(_.platform.supervisor.value.map {
        case v: Chunk[Fiber.Runtime[Any, Any]] if v.isEmpty || v.head.isInstanceOf[Fiber.Runtime[Any, Any]] => v
        case _                                                                                              => Chunk.empty
      })
  }

  implicit class RuntimeEnvOps[R <: Has[_]](val runtime: zio.Runtime[R]) extends AnyVal {
    def withFiberTracking(mode: FiberTrackingMode)(implicit tag: zio.Tag[R]): zio.Runtime[R with FiberTracking] = {
      mode match {
        case FiberTrackingMode.NoTracking => runtime.map(_.union[FiberTracking](FiberTracking.NoTracking))
        case FiberTrackingMode.Tracking(weak) =>
          val supervisor = runtime.unsafeRunTask(Supervisor.track(weak))
          runtime
            .mapPlatform(_.withSupervisor(supervisor))
            .map(_.union[FiberTracking](FiberTracking.make(supervisor)))
      }
    }
  }

  sealed trait FiberTrackingMode
  object FiberTrackingMode {
    case object NoTracking                     extends FiberTrackingMode
    case class Tracking(weak: Boolean = false) extends FiberTrackingMode
  }
}
