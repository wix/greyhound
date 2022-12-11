package greyhound

import com.wixpress.dst.greyhound.testkit.ManagedKafkaConfig
import zio.{RIO, Ref, Task, UIO, URIO, ZIO}

object Register {

  trait Service {
    def add(host: String, port: Int): Task[Unit]

    def updateKafkaAddress(address: String): Task[Unit]

    val get: UIO[Database]
  }

  type Register = Register.Service

  def add(host: String, port: Int): RIO[Register, Unit] =
    ZIO.serviceWithZIO[Register.Service](_.add(host, port))

  def updateKafkaAddress(address: String): RIO[Register, Unit] =
    ZIO.serviceWithZIO[Register.Service](_.updateKafkaAddress(address))

  val get: URIO[Register, Database] =
    ZIO.serviceWithZIO[Register.Service](_.get)
}

case class RegisterLive(ref: Ref[Database]) extends Register.Service {

  override def add(host: String, port: Int): Task[Unit] =
    ref.update(_.updateHost(host, port))

  override def updateKafkaAddress(address: String): Task[Unit] =
    ref.update(_.updateKafkaAddress(address))

  override val get: UIO[Database] = ref.get
}

object RegisterLive {

  val Default = Ref
    .make(Database.Default)
    .map(RegisterLive(_))
}

case class Database(host: HostDetails, kafkaAddress: String) {
  def updateHost(host: String, port: Int): Database =
    copy(host = HostDetails(host, port))

  def updateKafkaAddress(address: String): Database =
    copy(kafkaAddress = address)
}

object Database {
  val Default = Database(HostDetails.Default, s"localhost:${ManagedKafkaConfig.Default.kafkaPort}")
}

case class HostDetails(host: String, port: Int)

object HostDetails {
  val Default = HostDetails("localhost", Ports.RegisterPort)
}
