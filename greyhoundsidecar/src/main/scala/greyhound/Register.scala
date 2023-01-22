package greyhound

import zio.{RIO, Ref, Task, UIO, URIO, ZIO}

object Register {

  trait Service {
    def add(host: String, port: Int): Task[Unit]

    val get: UIO[Database]
  }

  type Register = Register.Service

  def add(host: String, port: Int): RIO[Register, Unit] =
    ZIO.serviceWithZIO[Register.Service](_.add(host, port))

  val get: URIO[Register, Database] =
    ZIO.serviceWithZIO[Register.Service](_.get)
}

case class RegisterLive(ref: Ref[Database]) extends Register.Service {

  override def add(host: String, port: Int): Task[Unit] =
    ref.update(_.updateHost(host, port))

  override val get: UIO[Database] = ref.get
}

case class Database(host: HostDetails) {
  def updateHost(host: String, port: Int): Database =
    copy(host = HostDetails(host, port))
}

case class HostDetails(host: String, port: Int)

object HostDetails {
  val Default = HostDetails("localhost", Ports.RegisterPort)
}
