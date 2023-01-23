
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.HandleMessagesRequest
import greyhound.SidecarService
import sidecaruser._
import support.{ConnectionSettings, KafkaTestSupport, SidecarTestSupport, TestContext}
import zio._
import zio.logging.backend.SLF4J
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.junit.JUnitRunnableSpec

object SidecarServiceTest extends JUnitRunnableSpec with SidecarTestSupport with KafkaTestSupport with ConnectionSettings {

  override val kafkaPort: Int = 6668
  override val zooKeeperPort: Int = 2188
  override val sideCarUserGrpcPort: Int = 9108

  val testSidecarUserLayer: ZLayer[Any, Nothing, TestSidecarUser] = ZLayer.fromZIO( for {
    ref <- Ref.make[Seq[HandleMessagesRequest]](Nil)
  } yield new TestSidecarUser(ref))

  val sidecarUserServerLayer = testSidecarUserLayer >>> ZLayer.fromZIO(for {
    user <- ZIO.service[TestSidecarUser]
    _ <- new TestServer(sideCarUserGrpcPort, user).myAppLogic.forkScoped
  } yield ())

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("sidecar service")(

      test("consume topic") {
        for {
          context <- ZIO.service[TestContext]
          sidecarUser <- ZIO.service[TestSidecarUser]
          sidecarService <- ZIO.service[SidecarService]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          _ <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString))
          _ <- sidecarService.startConsuming(StartConsumingRequest(Seq(Consumer(context.consumerId, context.group, context.topicName))))
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          records <- sidecarUser.collectedRecords.delay(6.seconds)
        } yield assert(records.nonEmpty)(equalTo(true))
      } @@ TestAspect.withLiveClock,

    ).provideLayer(
      Runtime.removeDefaultLoggers >>> SLF4J.slf4j ++
        testContextLayer ++
        ZLayer.succeed(zio.Scope.global) ++
        testSidecarUserLayer ++
        sidecarUserServerLayer ++
        sidecarServiceLayer(kafkaAddress)) @@
      runKafka(kafkaPort, zooKeeperPort)
}
