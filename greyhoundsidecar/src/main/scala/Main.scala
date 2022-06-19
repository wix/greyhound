
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.{DebugMetrics, Ports, SidecarClient, SidecarServerMain}
import zio._
import zio.console.{getStrLn, putStrLn}

object Main extends App {

  def initSidecarServer(kafkaAddress: String) = {
    println(s"~~~ INIT Sidecar Server with kafka address $kafkaAddress")
    new SidecarServerMain(kafkaAddress).myAppLogic.forkDaemon
  }

  val initKafka = ManagedKafka.make(ManagedKafkaConfig.Default)
    .provideCustomLayer(DebugMetrics.layer)
    .useForever
    .forkDaemon

  def startConsuming(topic: String, group: String) =  SidecarClient.managed.use { client =>
    client.startConsuming(StartConsumingRequest(Seq(Consumer("id1", group, topic))))
  }

  def createTopic(topic: String) = SidecarClient.managed.use { client =>
    client.createTopics(CreateTopicsRequest(Seq(TopicToCreate(topic, Some(1)))))
  }

  def produce(topic: String) = SidecarClient.managed.use { client =>

    val produceRequest = ProduceRequest(
      topic = topic,
      payload = Some("test-payload"),
      target = ProduceRequest.Target.Key("key"))

    client.produce(produceRequest)
  }

  val register = SidecarClient.managed.use { client =>
    client.register(RegisterRequest(
      host = "localhost",
      port = Ports.RegisterPort.toString))
  }

  val defaultKafkaAddress = s"localhost:${ManagedKafkaConfig.Default.kafkaPort}"
  val kafkaAddress: Option[String] = scala.util.Properties.envOrNone("KAFKA_ADDRESS")

  val greyhoundProduceApp = for {
    _ <- initKafka.when(kafkaAddress.isEmpty)
    _ <- initSidecarServer(kafkaAddress getOrElse defaultKafkaAddress)
//    topic = "test-topic"
//    _ <- createTopic(topic)
//    _ <- register
//    _ <- startConsuming(topic, "test-consumer")
//    _ <- produce(topic)
    _ <- putStrLn("~~~ WAITING FOR USER INPUT")
    _ <- getStrLn
  } yield scala.io.StdIn.readLine()

  override def run(args: List[String]) = {
    greyhoundProduceApp.exitCode
  }
}
