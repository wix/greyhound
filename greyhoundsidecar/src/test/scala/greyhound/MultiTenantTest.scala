package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.HandleMessagesRequest
import greyhound.sidecaruser.{TestServer, TestSidecarUser}
import greyhound.support.{ConnectionSettings, KafkaTestSupport, SidecarTestSupport, TestContext}
import io.grpc.Status
import zio.logging.backend.SLF4J
import zio.test.TestAspect.sequential
import zio.test.junit.JUnitRunnableSpec
import zio.test.{Spec, TestAspect, TestEnvironment, assertTrue}
import zio.{Ref, Runtime, Scope, ZIO, ZLayer, _}

import scala.util.Random.nextString


class TestSidecarUser1(consumedTopics: Ref[Seq[HandleMessagesRequest]]) extends TestSidecarUser(consumedTopics)
class TestSidecarUser2(consumedTopics: Ref[Seq[HandleMessagesRequest]]) extends TestSidecarUser(consumedTopics)

object MultiTenantTest extends JUnitRunnableSpec with SidecarTestSupport with KafkaTestSupport with ConnectionSettings {

  override val kafkaPort: Int = 6667
  override val zooKeeperPort: Int = 2187
  override val sideCarUserGrpcPort: Int = 9107
  val sideCarUser1GrpcPort = 9105
  val sideCarUser2GrpcPort = 9106

  val testSidecarUser1Layer: ZLayer[Any, Nothing, TestSidecarUser1] = ZLayer.fromZIO(for {
    ref <- Ref.make[Seq[HandleMessagesRequest]](Nil)
  } yield new TestSidecarUser1(ref))

  val sidecarUserServer1Layer = testSidecarUser1Layer >>> ZLayer.fromZIO(for {
    user <- ZIO.service[TestSidecarUser1]
    _ <- new TestServer(sideCarUser1GrpcPort, user).myAppLogic.forkScoped
  } yield ())

  val testSidecarUser2Layer: ZLayer[Any, Nothing, TestSidecarUser2] = ZLayer.fromZIO(for {
    ref <- Ref.make[Seq[HandleMessagesRequest]](Nil)
  } yield new TestSidecarUser2(ref))

  val sidecarUserServer2Layer = testSidecarUser2Layer >>> ZLayer.fromZIO(for {
    user <- ZIO.service[TestSidecarUser2]
    _ <- new TestServer(sideCarUser2GrpcPort, user).myAppLogic.forkScoped
  } yield ())

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("multi tenant")(
      test("two sidecar users") {
        val contextForUser1 = TestContext.random
        val contextForUser2 = TestContext.random
        for {
          sidecarUser1 <- ZIO.service[TestSidecarUser1]
          sidecarUser2 <- ZIO.service[TestSidecarUser2]
          sidecarService <- ZIO.service[SidecarService]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(contextForUser1.topicName, contextForUser1.partition))))
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(contextForUser2.topicName, contextForUser2.partition))))
          user1TenantId <- sidecarService.register(RegisterRequest(localhost, sideCarUser1GrpcPort.toString)).map(_.registrationId)
          user2TenantId <- sidecarService.register(RegisterRequest(localhost, sideCarUser2GrpcPort.toString)).map(_.registrationId)
          _ <- sidecarService.startConsuming(StartConsumingRequest(
            registrationId = user1TenantId,
            consumers = Seq(Consumer(contextForUser1.consumerId, contextForUser1.group, contextForUser1.topicName))))
          _ <- sidecarService.startConsuming(StartConsumingRequest(
            registrationId = user2TenantId,
            consumers = Seq(Consumer(contextForUser2.consumerId, contextForUser2.group, contextForUser2.topicName))))
          _ <- sidecarService.produce(ProduceRequest(contextForUser1.topicName, contextForUser1.payload, contextForUser1.target))
          _ <- sidecarService.produce(ProduceRequest(contextForUser2.topicName, contextForUser2.payload, contextForUser2.target))
          recordsUser1 <- sidecarUser1.collectedRequests.delay(6.seconds)
          recordsUser2 <- sidecarUser2.collectedRequests
        } yield assertTrue(recordsUser1.head.records.head.payload == contextForUser1.payload) &&
          assertTrue(recordsUser2.head.records.head.payload == contextForUser2.payload)
      },

      test("fail start consuming for non existing tenant") {
        for {
          sidecarService <- ZIO.service[SidecarService]
          result <- sidecarService.startConsuming(StartConsumingRequest(
            registrationId = nextString(10),
            consumers = Seq.empty)).either
        } yield assertTrue(result.left.get.getCode == Status.NOT_FOUND.getCode)
      }
    ).provideLayer(
      Runtime.removeDefaultLoggers >>> SLF4J.slf4j ++
        testContextLayer ++
        ZLayer.succeed(zio.Scope.global) ++
        testSidecarUser1Layer ++
        sidecarUserServer1Layer ++
        testSidecarUser2Layer ++
        sidecarUserServer2Layer ++
        sidecarServiceLayer(kafkaAddress)) @@ TestAspect.withLiveClock @@
      runKafka(kafkaPort, zooKeeperPort) @@ sequential
}
