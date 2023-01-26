package greyhound

import zio.{Ref, Task, UIO, ZLayer}

trait Register {
  def add(tenantId: String, host: String, port: Int): Task[Unit]

  def get(tenantId: String): UIO[Option[HostDetails]]
}

case class RegisterLive(ref: Ref[Map[String, HostDetails]]) extends Register {
  override def add(tenantId: String, host: String, port: Int): Task[Unit] =
    ref.update(_.updated(tenantId, HostDetails(host, port)))

  override def get(tenantId: String): UIO[Option[HostDetails]] = ref.get.map(_.get(tenantId))
}

object RegisterLive {
  val layer: ZLayer[Any, Nothing, RegisterLive] = ZLayer.fromZIO {
    Ref.make(Map.empty[String, HostDetails])
      .map(RegisterLive(_))
  }
}

case class HostDetails(host: String, port: Int)
