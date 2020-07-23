package com.wixpress.dst.greyhound.testkit

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ReportingProducer}
import com.wixpress.dst.greyhound.core.testkit.TestMetrics
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import zio.duration._
import zio.{ZIO, ZManaged, random, test}
import zio._

object ITEnv {
  type Env = TestMetrics with zio.ZEnv
  case class TestResources(kafka: ManagedKafka, producer: ReportingProducer)

  def ManagedEnv: UManaged[zio.ZEnv with TestMetrics] =
    for {
      env <- (GreyhoundMetrics.liveLayer ++ test.environment.liveEnvironment).build
      testMetrics <- TestMetrics.make
    } yield env ++ testMetrics

  def testResources(): ZManaged[zio.ZEnv with TestMetrics, Throwable, TestResources] = {
    for {
      kafka <- ManagedKafka.make(ManagedKafkaConfig.Default)
      producer <- Producer.make(ProducerConfig(kafka.bootstrapServers))
    } yield TestResources(kafka, ReportingProducer(producer))
  }

  def clientId = randomId.map(id => s"greyhound-consumers-$id")

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
