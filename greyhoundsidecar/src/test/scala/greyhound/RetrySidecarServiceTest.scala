package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import greyhound.sidecaruser.{FailOnceTestSidecarUser, TestServer}
import greyhound.support.{ConnectionSettings, KafkaTestSupport, TestContext}
import zio.test.Assertion.equalTo
import zio.test.TestAspect.sequential
import zio.test.junit.JUnitRunnableSpec
import zio.test.{Spec, TestAspect, TestEnvironment, assert}
import zio.{Scope, ZIO, ZLayer, _}

// TODO: merge this test suite with SidecarServiceTest when multi-tenancy is implemented
object RetrySidecarServiceTest extends JUnitRunnableSpec with KafkaTestSupport with ConnectionSettings {

  override val kafkaPort: Int = 6669
  override val zooKeeperPort: Int = 2189
  override val sideCarUserGrpcPort: Int = 9109

  val sidecarUserServerLayer = ZLayer.fromZIO(for {
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
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          request = StartConsumingRequest(
            registrationId = registrationId,
            consumers = Seq(
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
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          request = StartConsumingRequest(
            registrationId = registrationId,
            consumers = Seq(
              Consumer(context.consumerId, context.group, context.topicName, RetryStrategy.Blocking(BlockingRetry(10000)))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          recordsBeforeInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 6)
          _ <- assert(recordsBeforeInterval.isEmpty)(equalTo(true))
          recordsAfterInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 10)
        } yield assert(recordsAfterInterval.nonEmpty)(equalTo(true))
      },

      test("consume when retry strategy is NonBlockingRetry") {
        for {
          context <- ZIO.service[TestContext]
          sidecarService <- ZIO.service[SidecarService]
          failOnceSidecarUserService <- ZIO.service[FailOnceTestSidecarUser]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          request = StartConsumingRequest(
            registrationId = registrationId,
            consumers = Seq(
              Consumer(context.consumerId, context.group, context.topicName, RetryStrategy.NonBlocking(NonBlockingRetry(intervals = Seq(5000), partitions = Some(1))))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          recordsAfterInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 10)
        } yield assert(recordsAfterInterval.nonEmpty)(equalTo(true))
      },

      test("fail batch consume when retry strategy is NoRetry") {
        for {
          context <- ZIO.service[TestContext]
          sidecarService <- ZIO.service[SidecarService]
          failOnceSidecarUserService <- ZIO.service[FailOnceTestSidecarUser]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          request = StartConsumingRequest(
            registrationId = registrationId,
            batchConsumers = Seq(
              BatchConsumer(context.consumerId, context.group, context.topicName, BatchConsumer.RetryStrategy.NoRetry(NoRetry()))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          records <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 6)
        } yield assert(records.isEmpty)(equalTo(true))
      },

      test("batch consume when retry strategy is BlockingRetry with interval") {
        for {
          context <- ZIO.service[TestContext]
          sidecarService <- ZIO.service[SidecarService]
          failOnceSidecarUserService <- ZIO.service[FailOnceTestSidecarUser]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          request = StartConsumingRequest(
            registrationId = registrationId,
            batchConsumers = Seq(
              BatchConsumer(context.consumerId, context.group, context.topicName, BatchConsumer.RetryStrategy.Blocking(BlockingRetry(10000)))))
          _ <- sidecarService.startConsuming(request)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          recordsBeforeInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 6)
          _ <- assert(recordsBeforeInterval.isEmpty)(equalTo(true))
          recordsAfterInterval <- getSuccessfullyHandledRecords(failOnceSidecarUserService, delay = 10)
        } yield assert(recordsAfterInterval.nonEmpty)(equalTo(true))
      },

    ).provideLayer(
      TestContext.layer ++
        ZLayer.succeed(zio.Scope.global) ++
        FailOnceTestSidecarUser.layer ++
        (FailOnceTestSidecarUser.layer >>> sidecarUserServerLayer) ++
        ((TestTenantRegistry.layer ++ TestKafkaInfo.layer ++ (TestTenantRegistry.layer >>> ConsumerCreatorImpl.layer)) >>> SidecarService.layer)) @@
      TestAspect.withLiveClock @@
      runKafka(kafkaPort, zooKeeperPort) @@
      sequential

  private def getSuccessfullyHandledRecords(failOnceSidecarUserService: FailOnceTestSidecarUser, delay: Int) = {
    failOnceSidecarUserService.collectedRequests.delay(delay.seconds)
  }
}
