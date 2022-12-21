
import com.wixpress.dst.greyhound.core.admin.{AdminClient, AdminClientConfig}
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest.Target
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.HandleMessagesRequest
import greyhound.{HostDetails, SidecarService}
import sidecaruser._
import support.{KafkaTestSupport, SidecarTestSupport, TestContext}
import zio.test.Assertion.equalTo
import zio.test._
import zio.logging.backend.SLF4J
import zio.test.junit.JUnitRunnableSpec
import zio._


object SidecarServiceTest extends JUnitRunnableSpec with SidecarTestSupport with KafkaTestSupport {

  val sidecarUserLayer: ZLayer[Any, Nothing, SidecarUserServiceTest] = ZLayer.fromZIO( for {
    ref <- Ref.make[Seq[HandleMessagesRequest]](Nil)
  } yield new SidecarUserServiceTest(ref))

  val sidecarUserServer: ZIO[Any with Scope with SidecarUserServiceTest, Throwable, Nothing] = for {
    user <- ZIO.service[SidecarUserServiceTest]
    userService <- new SidecarUserServiceTestServer(9100, user).myAppLogic
  } yield userService

  var producedCalled = false
  val onProduceListener: ProducerRecord[Any, Any] => UIO[Unit] = (r: ProducerRecord[Any, Any]) => {
    producedCalled = true
    ZIO.log(s"produced record: $r")
  }
  val sideCar: SidecarService = new SidecarService(DefaultRegister, onProduceListener)


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

      test("produce event") {
        for {
          context <- ZIO.service[TestContext]
          _ <- sideCar.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
          _ <- sideCar.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
        } yield assert(producedCalled)(equalTo(true))
      },

      test("consume topic") {
        for {
          fork <- sidecarUserServer.forkDaemon
          context <- ZIO.service[TestContext]
          sidecarUser <- ZIO.service[SidecarUserServiceTest]
          _ <- sideCar.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
          _ <- sideCar.register(RegisterRequest(localHost, "9100"))
          _ <- sideCar.startConsuming(StartConsumingRequest(Seq(Consumer("1", "group", context.topicName))))
          _ <- sideCar.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
          records <- sidecarUser.collectedRecords.get.delay(6.seconds)
          _ <- fork.interrupt
        } yield assert(records.nonEmpty)(equalTo(true))
      } @@ TestAspect.withLiveClock,

    ).provideLayer(
      Runtime.removeDefaultLoggers >>> SLF4J.slf4j ++
      testContextLayer ++
      ZLayer.succeed(zio.Scope.global) ++
      sidecarUserLayer) @@
      runKafka @@
      closeKafka
}






