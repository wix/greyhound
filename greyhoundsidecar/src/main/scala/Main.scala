
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar.Consumer.RetryStrategy
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.{DebugMetrics, EnvArgs, Ports, SidecarClient, SidecarServerMain, SidecarUserServerMain}
import zio._
import zio.console.{getStrLn, putStrLn}

object Main extends App {

  val initSidecarServer = SidecarServerMain.myAppLogic.forkDaemon

  val initSidecarUserServer = SidecarUserServerMain.myAppLogic.forkDaemon

  val initKafka = ManagedKafka.make(ManagedKafkaConfig.Default)
    .provideCustomLayer(DebugMetrics.layer)
    .useForever
    .forkDaemon
    .whenM(EnvArgs.kafkaAddress.map(_.isEmpty))

  def startConsuming(topic: String, group: String, retryStrategy: RetryStrategy = RetryStrategy.NoRetry(NoRetry())) =  SidecarClient.managed.use { client =>
    client.startConsuming(StartConsumingRequest(Seq(Consumer("id1", group, topic, retryStrategy))))
  }

  def createTopic(topic: String) = SidecarClient.managed.use { client =>
    client.createTopics(CreateTopicsRequest(Seq(TopicToCreate(topic, Some(1)))))
  }

  def produce(topic: String, payload: String) = SidecarClient.managed.use { client =>

    val produceRequest = ProduceRequest(
      topic = topic,
      payload = Some(payload),
      target = ProduceRequest.Target.Key("key"))

    client.produce(produceRequest)
  }

  val register = SidecarClient.managed.use { client =>
    client.register(RegisterRequest(
      host = "localhost",
      port = Ports.RegisterPort.toString))
  }

  val greyhoundProduceApp = for {
    _ <- initKafka
    _ <- initSidecarServer
    _ <- initSidecarUserServer
    topic = "test-topic"
    _ <- createTopic(topic)
    _ <- register
    _ <- startConsuming(topic, "test-consumer", RetryStrategy.NonBlocking(NonBlockingRetry(Seq(1000, 2000, 3000))))
//    _ <- startConsuming(topic, "test-consumer", RetryStrategy.Blocking(BlockingRetry(1000)))
    _ <- putStrLn("~~~ ENTER MESSAGE")
    payload <- getStrLn
    _ <- produce(topic, payload)
    _ <- putStrLn("~~~ WAITING FOR USER INPUT")
    _ <- getStrLn
  } yield scala.io.StdIn.readLine()

  override def run(args: List[String]) = {
    greyhoundProduceApp.exitCode
  }
}
