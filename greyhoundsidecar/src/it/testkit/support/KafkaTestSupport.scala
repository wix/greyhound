package support

import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.DebugMetrics
import zio.ZLayer
import zio.test.TestAspect
import zio.test.TestAspect.{afterAll, beforeAll}

trait KafkaTestSupport {

  val kafkaPort = 6667
  val zooKeeperPort = 2181

  val kafkaConfig: ManagedKafkaConfig = ManagedKafkaConfig(kafkaPort, zooKeeperPort, Map.empty)
  val managedKafka = ManagedKafka.make(kafkaConfig).forever.forkDaemon
    .provideSomeLayer(DebugMetrics.layer ++ ZLayer.succeed(zio.Scope.global))


  def runKafka: TestAspect[Nothing, Any, Throwable, Any] = {
    beforeAll(zio.Console.printLine("staring kafka!!!").ignore *> managedKafka)
  }

  def closeKafka: TestAspect[Nothing, Any, Nothing, Any] = {
    afterAll(zio.Console.printLine("closing kafka!!!").ignore *> managedKafka.map(_.interrupt))
  }
}
