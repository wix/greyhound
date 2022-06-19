
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.sidecar.api.v1.greyhoundsidecar._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.{Ports, SidecarClient, SidecarServerMain}
import zio._
import zio.duration._

object Main extends App {

  val initSidecarServer = SidecarServerMain.myAppLogic.forkDaemon

  val initKafka = ManagedKafka.make(ManagedKafkaConfig.Default)
    .provideCustomLayer(GreyhoundMetrics.liveLayer)
    .useForever
    .forkDaemon

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

  val greyhoundProduceApp = for {
    _ <- initKafka
    _ <- initSidecarServer
    _ <- createTopic("test-topic")
    _ <- register
    _ <- produce("test-topic")
  } yield ()

  override def run(args: List[String]) =
    greyhoundProduceApp.exitCode
}
