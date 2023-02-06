package greyhound

import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import greyhound.sidecaruser.{TestServer, TestSidecarUser}
import greyhound.support.{ConnectionSettings, KafkaTestSupport, TestContext}
import zio.test.Assertion.equalTo
import zio.test.junit.JUnitRunnableSpec
import zio.test.{Spec, TestAspect, TestEnvironment, assert}
import zio.{Scope, ZIO, ZLayer}
import zio._
import zio.test.TestAspect.sequential

object SidecarServiceTest extends JUnitRunnableSpec with KafkaTestSupport with ConnectionSettings {

  override val kafkaPort: Int = 6668
  override val zooKeeperPort: Int = 2188
  override val sideCarUserGrpcPort: Int = 9108

  val sidecarUserServerLayer = ZLayer.fromZIO(for {
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
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          _ <- sidecarService.startConsuming(StartConsumingRequest(
            registrationId = registrationId,
            consumers = Seq(Consumer(context.consumerId, context.group, context.topicName))))
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
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          _ <- sidecarService.startConsuming(StartConsumingRequest(
            registrationId = registrationId,
            batchConsumers = Seq(BatchConsumer(
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
      test("stop consume topic"){
        for {
          context <- ZIO.service[TestContext]
          sidecarUser <- ZIO.service[TestSidecarUser]
          sidecarService <- ZIO.service[SidecarService]
          _ <- sidecarService.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, context.partition))))
          registrationId <- sidecarService.register(RegisterRequest(localhost, sideCarUserGrpcPort.toString)).map(_.registrationId)
          consumers = Seq(Consumer(context.consumerId, context.group, context.topicName))
          consumersDetails = consumers.map(consumer => ConsumerDetails(topic = consumer.topic, group = consumer.group))
          _ <- sidecarService.startConsuming(StartConsumingRequest(registrationId = registrationId, consumers = consumers))
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          _ <- sidecarService.stopConsuming(StopConsumingRequest(registrationId = registrationId, consumerDetails = consumersDetails)).delay(1.second)
          _ <- sidecarService.produce(ProduceRequest(context.topicName, context.payload, context.target))
          records <- sidecarUser.collectedRequests.delay(6.seconds)
        } yield assert(records.size)(equalTo(1))
      },
    ).provideLayer(
      TestContext.layer ++
        ZLayer.succeed(zio.Scope.global) ++
        TestSidecarUser.layer ++
        (TestSidecarUser.layer >>> sidecarUserServerLayer) ++
        ((ConsumerRegistryLive.layer ++ RegisterLive.layer ++ TestKafkaInfo.layer ++ (ConsumerRegistryLive.layer >>> ConsumerCreatorImpl.layer)) >>> SidecarService.layer)) @@
      TestAspect.withLiveClock @@
      runKafka(kafkaPort, zooKeeperPort) @@
      sequential
}
