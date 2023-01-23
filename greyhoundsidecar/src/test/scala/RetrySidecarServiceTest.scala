
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest.Target
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.HandleMessagesRequest
import greyhound.SidecarService
import sidecaruser._
import support.{ConnectionSettings, KafkaTestSupport, SidecarTestSupport, TestContext}
import zio._
import zio.logging.backend.SLF4J
import zio.test.Assertion.equalTo
import zio.test.TestAspect.sequential
import zio.test._
import zio.test.junit.JUnitRunnableSpec

// TODO: merge this test suite with SidecarServiceTest when multi-tenancy is implemented
object RetrySidecarServiceTest extends JUnitRunnableSpec with SidecarTestSupport with KafkaTestSupport with ConnectionSettings {

  override val kafkaPort: Int = 6669
  override val zooKeeperPort: Int = 2189
  override val sideCarUserGrpcPort: Int = 9109

  val testSidecarUserLayer: ZLayer[Any, Nothing, FailOnceTestSidecarUser] = ZLayer.fromZIO(for {
    messageSinkRef <- Ref.make[Seq[HandleMessagesRequest]](Nil)
    shouldFailRef <- Ref.make[Boolean](true)
  } yield new FailOnceTestSidecarUser(messageSinkRef, shouldFailRef))

  val sidecarUserServerLayer = testSidecarUserLayer >>> ZLayer.fromZIO(for {
    user <- ZIO.service[FailOnceTestSidecarUser]
    _ <- new TestServer(sideCarUserGrpcPort, user).myAppLogic.forkScoped
  } yield ())

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("sidecar service")(
      test("fail when retry strategy is NoRetry") {
        for {
          context <- ZIO.service[TestContext]
          sidecarService <- ZIO.service[SidecarService]
          failOnceSidecarUserService <- ZIO.service[FailOnceTestSidecarUser]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          _ <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString))
          request = StartConsumingRequest(Seq(
            Consumer(context.consumerId, context.group, context.topicName, RetryStrategy.NoRetry(NoRetry()))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          records <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 6)
        } yield assert(records.isEmpty)(equalTo(true))
      },

      test("consume when retry strategy is BlockingRetry with interval") {
        for {
          context <- ZIO.service[TestContext]
          sidecarService <- ZIO.service[SidecarService]
          failOnceSidecarUserService <- ZIO.service[FailOnceTestSidecarUser]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          _ <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString))
          request = StartConsumingRequest(Seq(
            Consumer(context.consumerId, context.group, context.topicName, RetryStrategy.Blocking(BlockingRetry(10000)))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          recordsBeforeInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 6)
          _ <- assert(recordsBeforeInterval.isEmpty)(equalTo(true))
          recordsAfterInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 10)
        } yield assert(recordsAfterInterval.nonEmpty)(equalTo(true))
      }

    ).provideLayer(
      Runtime.removeDefaultLoggers >>> SLF4J.slf4j ++
        testContextLayer ++
        ZLayer.succeed(zio.Scope.global) ++
        testSidecarUserLayer ++
        sidecarServiceLayer(kafkaAddress) ++
        sidecarUserServerLayer) @@ TestAspect.withLiveClock @@ runKafka(kafkaPort, zooKeeperPort) @@ sequential

  private def getSuccessfullyHandledRecords(failOnceSidecarUserService: FailOnceTestSidecarUser, delay: Int) = {
    failOnceSidecarUserService.collectedRecords.delay(delay.seconds)
  }
}
