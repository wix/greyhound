package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.HandleMessagesRequest
import greyhound.sidecaruser.{TestServer, TestSidecarUser}
import greyhound.support.{ConnectionSettings, KafkaTestSupport, SidecarTestSupport, TestContext}
import zio.logging.backend.SLF4J
import zio.test.Assertion.equalTo
import zio.test.junit.JUnitRunnableSpec
import zio.test.{Spec, TestAspect, TestEnvironment, assert}
import zio.{Ref, Runtime, Scope, ZIO, ZLayer}
import zio._
import zio.test.TestAspect.sequential

object SidecarServiceTest extends JUnitRunnableSpec with SidecarTestSupport with KafkaTestSupport with ConnectionSettings {

  override val kafkaPort: Int = 6668
  override val zooKeeperPort: Int = 2188
  override val sideCarUserGrpcPort: Int = 9108

  val testSidecarUserLayer: ZLayer[Any, Nothing, TestSidecarUser] = ZLayer.fromZIO(for {
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
          records <- sidecarUser.collectedRequests.delay(6.seconds)
        } yield assert(records.nonEmpty)(equalTo(true))
      },
      test("batch consume topic") {
        for {
          context <- ZIO.service[TestContext]
          sidecarUser <- ZIO.service[TestSidecarUser]
          sidecarService <- ZIO.service[SidecarService]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          _ <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString))
          _ <- sidecarService.startConsuming(StartConsumingRequest(batchConsumers = Seq(BatchConsumer(
            id = context.consumerId, group = context.group, topic = context.topicName, extraProperties =
              Map("fetch.min.bytes" -> 10000.toString, // This means the consumer will try to accumulate 10000 bytes
                "fetch.max.wait.ms" -> 5000.toString   // If it doesn't get to 10000 bytes it will wait up to 5 seconds and then fetch what it has
              )))))
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          requests <- sidecarUser.collectedRequests.delay(15.seconds)
          _ <- zio.Console.printLine(requests).orDie
          records = requests.head.records
        } yield assert(records.size)(equalTo(2))
      },
    ).provideLayer(
      Runtime.removeDefaultLoggers >>> SLF4J.slf4j ++
        testContextLayer ++
        ZLayer.succeed(zio.Scope.global) ++
        testSidecarUserLayer ++
        sidecarUserServerLayer ++
        sidecarServiceLayer(kafkaAddress)) @@ TestAspect.withLiveClock @@
      runKafka(kafkaPort, zooKeeperPort) @@ sequential
}
