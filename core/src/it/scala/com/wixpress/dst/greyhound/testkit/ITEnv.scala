package com.wixpress.dst.greyhound.testkit

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ReportingProducer}
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.random.Random
import zio.{ZIO, ZManaged, random, test}

object ITEnv {
  type Env = GreyhoundMetrics with zio.ZEnv
  case class TestResources(kafka: ManagedKafka, producer: ReportingProducer)

  def ManagedEnv =
    (GreyhoundMetrics.liveLayer ++ test.environment.liveEnvironment).build

  def testResources(): ZManaged[Blocking with GreyhoundMetrics, Throwable, TestResources] = {
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
    def createRandomTopic(partitions: Int = partitions, prefix: String = "topic") = for {
      topic <- randomId.map(id => s"$prefix-$id")
      _ <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete))
    } yield topic
  }
}
