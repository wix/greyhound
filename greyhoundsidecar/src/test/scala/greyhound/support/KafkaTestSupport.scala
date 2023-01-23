package greyhound.support

import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import greyhound.DebugMetrics
import zio.ZLayer
import zio.test.TestAspect
import zio.test.TestAspect.beforeAll

trait KafkaTestSupport {
  def runKafka(kafkaPort: Int, zooKeeperPort: Int): TestAspect[Nothing, Any, Throwable, Any] = {
    val kafkaConfig: ManagedKafkaConfig = ManagedKafkaConfig(kafkaPort, zooKeeperPort, Map.empty)
    val managedKafka = ManagedKafka.make(kafkaConfig).interruptible.forkScoped
      .provideSomeLayer(DebugMetrics.layer ++ ZLayer.succeed(zio.Scope.global))

    beforeAll(zio.Console.printLine("staring kafka!!!").ignore *> managedKafka)
  }
}
