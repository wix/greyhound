package greyhound

import zio.{RIO, Ref, Task, UIO, URIO, ZIO}

object Register {

  trait Service {
    def add(tenantId: String, host: String, port: Int): Task[Unit]

    def get(tenantId: String): UIO[Option[HostDetails]]
  }

  type Register = Register.Service

  def add(tenantId: String, host: String, port: Int): RIO[Register, Unit] =
    ZIO.serviceWithZIO[Register.Service](_.add(tenantId, host, port))

  def get(tenantId: String): URIO[Register, Option[HostDetails]] =
    ZIO.serviceWithZIO[Register.Service](_.get(tenantId))
}

case class RegisterLive(ref: Ref[Map[String, HostDetails]]) extends Register.Service {

  override def add(tenantId: String, host: String, port: Int): Task[Unit] =
    ref.update(_.updated(tenantId, HostDetails(host, port)))

  override def get(tenantId: String): UIO[Option[HostDetails]] = ref.get.map(_.get(tenantId))
}

case class HostDetails(host: String, port: Int)
