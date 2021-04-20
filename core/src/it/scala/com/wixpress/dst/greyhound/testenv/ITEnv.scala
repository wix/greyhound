package com.wixpress.dst.greyhound.testenv

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ReportingProducer}
import com.wixpress.dst.greyhound.core.testkit.TestMetrics
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import zio.duration._
import zio.{UManaged, ZIO, ZManaged, random, test}

object ITEnv {
  type Env = TestMetrics with zio.ZEnv
  case class TestResources(kafka: ManagedKafka, producer: ReportingProducer[Any])

  def ManagedEnv: UManaged[zio.ZEnv with TestMetrics] =
    for {
      env <- (GreyhoundMetrics.liveLayer ++ test.environment.liveEnvironment).build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  def testResources(): ZManaged[zio.ZEnv with TestMetrics, Throwable, TestResources] = {
    for {
      kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
      producer <- Producer.makeR[Any](ProducerConfig(kafka.bootstrapServers))
    } yield TestResources(kafka, ReportingProducer(producer))
  }

  def clientId = randomId.map(id => s"greyhound-consumers-$id")
  def clientId(tag: String) = randomId.map(id => s"client-$tag-$id")

  val partitions = 4
  val delete = CleanupPolicy.Delete(1.hour.toMillis)

  def randomAlphaLowerChar = {
    val low = 'a'.toInt
    val high = 'z'.toInt + 1
    random.nextIntBetween(low, high).map(_.toChar)
  }

  val randomId = ZIO.collectAll(List.fill(6)(randomAlphaLowerChar)).map(_.mkString)
  val randomGroup = randomId.map(id => s"group-$id")

  implicit class ManagedKafkaOps(val kafka: ManagedKafka) {
    def createRandomTopic(partitions: Int = partitions, prefix: String = "topic", params: Map[String, String] = Map.empty) = for {
      topic <- randomId.map(id => s"$prefix-$id")
      _ <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete, params))
    } yield topic
  }
}
