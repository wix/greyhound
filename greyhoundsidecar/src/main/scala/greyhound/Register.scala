package greyhound

import zio.{RIO, Ref, Task, UIO, URIO, ZIO}

object Register {

  trait Service {
    def add(host: String, port: Int): Task[Unit]

    val get: UIO[HostDetails]
  }

  type Register = Register.Service

  def add(host: String, port: Int): RIO[Register, Unit] =
    ZIO.serviceWithZIO[Register.Service](_.add(host, port))

  val get: URIO[Register, HostDetails] =
    ZIO.serviceWithZIO[Register.Service](_.get)
}

case class RegisterLive(ref: Ref[HostDetails]) extends Register.Service {

  override def add(host: String, port: Int): Task[Unit] =
    ref.update(_.copy(host, port))

  override val get: UIO[HostDetails] = ref.get
}

case class HostDetails(host: String, port: Int)
