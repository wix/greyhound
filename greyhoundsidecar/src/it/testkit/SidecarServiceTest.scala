
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.ProduceRequest.Target
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecaruser.HandleMessagesRequest
import greyhound.SidecarService
import sidecaruser._
import support.{KafkaTestSupport, SidecarTestSupport, TestContext}
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.junit.JUnitRunnableSpec
import zio.{Ref, Scope, UIO, ZIO, ZLayer}


class SidecarServiceTest extends JUnitRunnableSpec with SidecarTestSupport with KafkaTestSupport {

  val sidecarUserLayer: ZLayer[Any, Nothing, SidecarUserServiceTest] = ZLayer.fromZIO( for {
    ref <- Ref.make[Seq[HandleMessagesRequest]](Nil)
  } yield new SidecarUserServiceTest(ref))

  val sidecarUserService: ZIO[Any with Scope with SidecarUserServiceTest, Throwable, Nothing] = for {
    user <- ZIO.service[SidecarUserServiceTest]
    userService <- new SidecarUserServiceTestMain(9100, user).myAppLogic
  } yield userService

  var producedCalled = false
  val onProduceListener: ProducerRecord[Any, Any] => UIO[Unit] = (_: ProducerRecord[Any, Any]) => {
    producedCalled = true
    zio.Console.printLine("topic produced").ignore *> ZIO.unit
  }
  val sideCar: SidecarService = new SidecarService(DefaultRegister, onProduceListener)


  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("sidecar service")(
//
//      test("register a sidecar user") {
//        for {
//          _ <- sideCar.register(RegisterRequest(localHost, "4567"))
//          db <- DefaultRegister.get
//        } yield assert(db.host)(equalTo(HostDetails(localHost, 4567)))
//      },
//
//      test("create new topic") {
//        for {
//          context <- ZIO.service[TestContext]
//          _ <- sideCar.createTopics(CreateTopicsRequest(Seq(TopicToCreate(context.topicName, Option(1)))))
//          db <- DefaultRegister.get
//          adminClient <- AdminClient.make(AdminClientConfig(db.kafkaAddress))
//          topicExist <- adminClient.topicExists(context.topicName)
//        } yield assert(topicExist)(equalTo(true))
//      },

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
          _ <- sideCar.startConsuming(StartConsumingRequest(Seq(Consumer("1", "group", context.topicName, RetryStrategy.NonBlocking(NonBlockingRetry(Seq(1000, 2000, 3000)))))))
          _ <- sideCar.produce(ProduceRequest(context.topicName, context.payload, context.topicKey.map(Target.Key).getOrElse(Target.Empty)))
          records <- sidecarUser.collectedRecords.get
          _ <- fork.interrupt
        } yield assert(records.nonEmpty)(equalTo(true))
      },

    ).provideLayer(testContextLayer ++ ZLayer.succeed(zio.Scope.global) ++ sidecarUserLayer) @@ runKafka @@ closeKafka


}






