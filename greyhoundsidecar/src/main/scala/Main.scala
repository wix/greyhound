
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.{DebugMetrics, EnvArgs, Ports, SidecarClient, SidecarServerMain, SidecarUserServerMain}
import zio._
import zio.ZIOAppDefault
import zio.Console.{printLine, readLine}

object Main extends ZIOAppDefault {
//
//  val initSidecarServer = SidecarServerMain.myAppLogic.forkDaemon
//
//  val initSidecarUserServer = SidecarUserServerMain.myAppLogic.forkDaemon
//
//  //  val initKafka = ManagedKafka.make(ManagedKafkaConfig.Default)
//  //    .provideCustomLayer(DebugMetrics.layer)
//  //    .useForever
//  //    .forkDaemon
//  //    .whenZIO(EnvArgs.kafkaAddress.map(_.isEmpty))
//
//  def startConsuming(topic: String, group: String, retryStrategy: RetryStrategy = RetryStrategy.NoRetry(NoRetry())) = ZIO.scoped {
//    for {
//      manageClient <- SidecarClient.managed
//      _ <- manageClient.startConsuming(StartConsumingRequest(
//        consumers = Seq(Consumer("id1", group, topic, retryStrategy)),
//        batchConsumers = Seq(BatchConsumer("id2", s"$group-batch", s"$topic-batch"))
//      ))
//    } yield ()
//  }
//
//  def createTopics(topic: String) = ZIO.scoped {
//    for {
//      manageClient <- SidecarClient.managed
//      _ <- manageClient.createTopics(CreateTopicsRequest(Seq(
//        TopicToCreate(topic, Some(1)),
//        TopicToCreate(s"$topic-batch", Some(1))
//      )))
//    } yield ()
//  }
//
//
//  def produce(topic: String, payload: String) = ZIO.scoped {
//    for {
//      manageClient <- SidecarClient.managed
//      produceRequest = ProduceRequest(
//        topic = topic,
//        payload = Some(payload),
//        target = ProduceRequest.Target.Key("key"))
//      _ <- manageClient.produce(produceRequest)
//    } yield ()
//  }
//
//
//  val register = ZIO.scoped {
//    for {
//      manageClient <- SidecarClient.managed
//      _ <- manageClient.register(RegisterRequest(
//        host = "localhost",
//        port = Ports.RegisterPort.toString))
//    } yield ()
//  }
//
//  val greyhoundProduceApp = for {
//    //  _ <- initKafka
//    _ <- initSidecarServer
//    _ <- initSidecarUserServer
//    topic = "test-topic"
//    _ <- createTopics(topic)
//    _ <- register
//    _ <- startConsuming(topic, "test-consumer", RetryStrategy.NonBlocking(NonBlockingRetry(Seq(1000, 2000, 3000))))
//    //    _ <- startConsuming(topic, "test-consumer", RetryStrategy.Blocking(BlockingRetry(1000)))
//    _ <- printLine("~~~ ENTER MESSAGE")
//    payload <- readLine
//    _ <- printLine(s"~~~ Producing to $topic")
//    _ <- produce(topic, payload)
//    _ <- printLine(s"~~~ Producing to $topic-batch")
//    _ <- produce(s"$topic-batch", s"$payload-batch")
//    _ <- printLine("~~~ WAITING FOR USER INPUT")
//    _ <- readLine
//  } yield scala.io.StdIn.readLine()

//  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] =
//    greyhoundProduceApp.exitCode

  override def run: ZIO[Any with ZIOAppArgs with Scope, Any, Any] = ???
}
