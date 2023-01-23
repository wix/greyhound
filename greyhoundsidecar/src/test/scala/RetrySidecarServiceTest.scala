
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
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


object RetrySidecarServiceTest extends JUnitRunnableSpec with SidecarTestSupport with KafkaTestSupport with ConnectionSettings {

  override val kafkaPort: Int = 6669
  override val zooKeeperPort: Int = 2189
  override val sideCarUserGrpcPort: Int = 9109

  val sidecarUserLayer: ZLayer[Any, Nothing, FailOnceSidecarUserService] = ZLayer.fromZIO(for {
    messageSinkRef <- Ref.make[Seq[HandleMessagesRequest]](Nil)
    shouldFailRef <- Ref.make[Boolean](true)
  } yield new FailOnceSidecarUserService(messageSinkRef, shouldFailRef))

  val sidecarUserServerLayer = sidecarUserLayer >>> ZLayer.fromZIO(for {
    user <- ZIO.service[FailOnceSidecarUserService]
    _ <- new SidecarUserServiceTestServer(sideCarUserGrpcPort, user).myAppLogic.forkScoped
  } yield ())

  val onProduceListener: ProducerRecord[Any, Any] => UIO[Unit] = (r: ProducerRecord[Any, Any]) => {
    ZIO.log(s"produced record: $r")
  }
  val sidecarServiceLayer = ZLayer.succeed(new SidecarService(DefaultRegister, onProduceListener, kafkaAddress))

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("sidecar service")(
      test("fail when retry strategy is NoRetry") {
        for {
          context <- ZIO.service[TestContext]
          sidecarService <- ZIO.service[SidecarService]
          failOnceSidecarUserService <- ZIO.service[FailOnceSidecarUserService]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
          _ <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString))
          request = StartConsumingRequest(Seq(
            Consumer("1", "group", context.topicName, RetryStrategy.NoRetry(NoRetry()))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
          records <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 6)
        } yield assert(records.isEmpty)(equalTo(true))
      },

      test("consume when retry strategy is BlockingRetry with interval") {
        for {
          context <- ZIO.service[TestContext]
          sidecarService <- ZIO.service[SidecarService]
          failOnceSidecarUserService <- ZIO.service[FailOnceSidecarUserService]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
          _ <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString))
          request = StartConsumingRequest(Seq(
            Consumer("1", "group", context.topicName, RetryStrategy.Blocking(BlockingRetry(10000)))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
          recordsBeforeInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 6)
          _ <- assert(recordsBeforeInterval.isEmpty)(equalTo(true))
          recordsAfterInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 10)
        } yield assert(recordsAfterInterval.nonEmpty)(equalTo(true))
      }

    ).provideLayer(
      Runtime.removeDefaultLoggers >>> SLF4J.slf4j ++
        testContextLayer ++
        ZLayer.succeed(zio.Scope.global) ++
        sidecarUserLayer ++
        sidecarServiceLayer ++
        sidecarUserServerLayer) @@ TestAspect.withLiveClock @@ runKafka(kafkaPort, zooKeeperPort) @@ sequential

  private def getSuccessfullyHandledRecords(failOnceSidecarUserService: FailOnceSidecarUserService, delay: Int) = {
    failOnceSidecarUserService.collectedRecords.get.delay(delay.seconds)
  }
}
