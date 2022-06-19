package greyhound

import zio.{Has, RIO, Ref, Task, UIO, ULayer, URIO, ZIO}

object Register {

  trait Service {
    def add(host: String, port: Int): Task[Unit]

    val get: UIO[HostDetails]
  }
  
  type Register = Has[Register.Service]
  
  def add(host: String, port: Int): RIO[Register, Unit] =
    ZIO.serviceWith[Register.Service](_.add(host, port))

  val get: URIO[Register, HostDetails] =
    ZIO.serviceWith[Register.Service](_.get)
}

case class RegisterLive(ref: Ref[HostDetails]) extends Register.Service {

  override def add(host: String, port: Int): Task[Unit] =
    ref.update(_.copy(host, port))

  override val get: UIO[HostDetails] = ref.get
}

object RegisterLive {

  val Layer: ULayer[Register.Register] = Ref.make(HostDetails.Default)
    .map(RegisterLive(_))
    .toLayer
}

case class Database(host: HostDetails)

case class HostDetails(host: String, port: Int)

object HostDetails {
  val Default = HostDetails("localhost", Ports.RegisterPort)
}
