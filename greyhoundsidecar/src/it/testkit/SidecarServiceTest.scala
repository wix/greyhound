
import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest.Target
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.{CreateTopicsRequest, ProduceRequest, RegisterRequest, TopicToCreate}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.Register.Register
import greyhound.{Database, DebugMetrics, HostDetails, SidecarService}
import zio.test.Assertion.equalTo
import zio.test.TestAspect.{afterAll, beforeAll}
import zio.test._
import zio.test.junit.JUnitRunnableSpec
import zio.{Ref, Scope, Task, UIO, ULayer, ZIO, ZLayer}


class SidecarServiceTest extends JUnitRunnableSpec {

  object DefaultRegister extends Register {

    private var db = Database(HostDetails("", 0), s"$localHost:$kafkaPort")

    override def add(host: String, port: Int): Task[Unit] = {
      db = db.copy(host = HostDetails(host, port))
      ZIO.unit
    }

    override def updateKafkaAddress(address: String): Task[Unit] = {
      db = db.copy(kafkaAddress = address)
      ZIO.unit
    }

    override val get: UIO[Database] = ZIO.succeed(db)
  }

  case class TestContext(topicName: String, payload: Option[String], topicKey: Option[String])


  object TestContext {

    import scala.util.Random.{nextInt, nextString}

    def random: TestContext = TestContext(topicName = s"topic-$nextInt", Option(nextString(10)), Option(nextString(5)))
  }

  val testContextLayer: ULayer[TestContext] = ZLayer.succeed(TestContext.random)

  val localHost = "localHost"
  val kafkaPort = 6667
  val zooKeeperPort = 2181

  var producedCalled =  false
  val onProduceListener: ProducerRecord[Any, Any] => UIO[Unit] = (_: ProducerRecord[Any, Any]) => {
    producedCalled = true
    ZIO.unit
  }
  val sideCar: SidecarService = new SidecarService(DefaultRegister, onProduceListener)

  val kafkaConfig: ManagedKafkaConfig = ManagedKafkaConfig(kafkaPort, zooKeeperPort, Map.empty)
  val managedKafka = ManagedKafka.make(kafkaConfig).forever.forkDaemon
    .provideSomeLayer(DebugMetrics.layer ++ ZLayer.succeed(zio.Scope.global))


  def runKafka: TestAspect[Nothing, Any, Throwable, Any] = {
    beforeAll(zio.Console.printLine("staring kafka!!!").ignore *> managedKafka)
  }

  def closeKafka: TestAspect[Nothing, Any, Nothing, Any] = {
    afterAll(zio.Console.printLine("closing kafka!!!").ignore *> managedKafka.map(_.interrupt))
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("sidecar service")(

      test("register a sidecar user") {
        for {
          _ <- sideCar.register(RegisterRequest(localHost, "4567"))
          db <- DefaultRegister.get
        } yield assert(db.host)(equalTo(HostDetails(localHost, 4567)))
      },

      test("create new topic") {
        for {
          context <- ZIO.service[TestContext]
          _ <- sideCar.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
          db <- DefaultRegister.get
          adminClient <- AdminClient.make(AdminClientConfig(db.kafkaAddress))
          topicExist <- adminClient.topicExists(context.topicName)
        } yield assert(topicExist)(equalTo(true))
      },

      test("produce") {
        for {
          context <- ZIO.service[TestContext]
          _ <- sideCar.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
          _ <- sideCar.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
        } yield assert(producedCalled)(equalTo(true))
      },

      test("echo 1") {
        assert(true)(equalTo(true))
      },
      test("echo 2") {
        assert(true)(equalTo(true))
      }
    ).provideLayer(testContextLayer ++ ZLayer.succeed(zio.Scope.global)) @@ runKafka @@ closeKafka


}


