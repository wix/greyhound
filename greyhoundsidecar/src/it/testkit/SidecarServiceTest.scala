
import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest.Target
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.ZioGreyhoundsidecaruser.RGreyhoundSidecarUser
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.{HandleMessagesRequest, HandleMessagesResponse}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.Register.Register
import greyhound.{Database, DebugMetrics, HostDetails, SidecarService}
import io.grpc.Status
import scalapb.zio_grpc.{ServerMain, ServiceList}
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

  val testContextLayer: ULayer[TestContext] = ZLayer.succeed(TestContext.random)

  val sidecarUserLayer: ZLayer[Any, Nothing, SidecarUserServiceTest] = ZLayer.fromZIO( for {
    ref <- Ref.make[Seq[HandleMessagesRequest]](Nil)
  } yield new SidecarUserServiceTest(ref))

  val sidecarUserService: ZIO[Any with Scope with SidecarUserServiceTest, Throwable, Nothing] = for {
    user <- ZIO.service[SidecarUserServiceTest]
    userService <- new SidecarUserServiceTestMain(9100, user).myAppLogic
  } yield userService

  val localHost = "localHost"
  val kafkaPort = 6667
  val zooKeeperPort = 2181

  var producedCalled = false
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

//      test("produce event") {
//        for {
//          context <- ZIO.service[TestContext]
//          _ <- sideCar.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
//          _ <- sideCar.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
//        } yield assert(producedCalled)(equalTo(true))
//      },

      test("consume topic") {
        for {
          fork <- sidecarUserService.forkDaemon
          context <- ZIO.service[TestContext]
          sidecarUser <- ZIO.service[SidecarUserServiceTest]
          _ <- sideCar.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
          _ <- sideCar.register(RegisterRequest(localHost, "9100"))
          _ <- sideCar.startConsuming(StartConsumingRequest(Seq(Consumer("1", "group", context.topicName))))
          _ <- sideCar.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
          records <- sidecarUser.collectedRecords.get
          _ <- fork.interrupt
        } yield assert(producedCalled)(equalTo(true))
      },

    ).provideLayer(testContextLayer ++ ZLayer.succeed(zio.Scope.global) ++ sidecarUserLayer) @@ runKafka @@ closeKafka


}

class SidecarUserServiceTestMain(servicePort: Int, impl: RGreyhoundSidecarUser[Any]) extends ServerMain {

  override def port: Int = servicePort

  override def services: ServiceList[Any] = ServiceList.add(impl)
}

class SidecarUserServiceTest(consumedTopics: Ref[Seq[HandleMessagesRequest]]) extends RGreyhoundSidecarUser[Any] {

  def collectedRecords: Ref[Seq[HandleMessagesRequest]] = consumedTopics

  override def handleMessages(request: HandleMessagesRequest): ZIO[Any, Status, HandleMessagesResponse] = {
    zio.Console.printLine("!!!!!          consume          !!!!!").orElse(ZIO.fail(Status.RESOURCE_EXHAUSTED)) *> consumedTopics.update(_ :+ request) *>  ZIO.succeed(HandleMessagesResponse())
  }


}


case class TestContext(topicName: String, payload: Option[String], topicKey: Option[String])

object TestContext {

  import scala.util.Random.{nextInt, nextString}

  def random: TestContext = TestContext(topicName = s"topic-$nextInt", Option(nextString(10)), Option(nextString(5)))
}