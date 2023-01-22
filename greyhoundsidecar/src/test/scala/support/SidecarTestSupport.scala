package support

import greyhound.{Database, HostDetails}
import greyhound.Register.Register
import zio.{Task, UIO, ULayer, ZIO, ZLayer}

trait SidecarTestSupport {

  val localhost = "localhost"

  val kafkaAddress = "localhost:6667"


  val testContextLayer: ULayer[TestContext] = ZLayer.succeed(TestContext.random)

  object DefaultRegister extends Register {

    private var db = Database(HostDetails("", 0))

    override def add(host: String, port: Int): Task[Unit] = {
      db = db.copy(host = HostDetails(host, port))
      ZIO.unit
    }

    override val get: UIO[Database] = ZIO.succeed(db)
  }
}
