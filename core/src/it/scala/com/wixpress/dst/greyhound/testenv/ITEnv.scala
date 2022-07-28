package com.wixpress.dst.greyhound.testenv

import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ReportingProducer}
import com.wixpress.dst.greyhound.core.testkit.{TestMetrics, TestMetricsEnvironment}
import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import zio.Random.nextIntBetween
import zio.{Random, Scope, Trace, ZEnvironment, ZIO, ZLayer, durationInt}
import zio.managed._
import zio.test.{Live, TestClock}

object ITEnv {
    implicit val trace = Trace.empty
  type Env = TestMetrics
  case class TestResources(kafka: ManagedKafka, producer: ReportingProducer[Any])

  def ManagedEnv: UManaged[ ZEnvironment[TestMetrics with GreyhoundMetrics]] =
    for {
      env         <- ZManaged.fromZIO(GreyhoundMetrics.liveLayer.build.provide(ZLayer.succeed(Scope.global)))
      testMetrics <- ZManaged.succeed(TestMetricsEnvironment)
      a = env ++ testMetrics
    } yield a

  def testResources(): ZManaged[TestMetrics with GreyhoundMetrics, Throwable, TestResources] = {
    val nextPort = nextIntBetween(10100, 60001)

    for {
      kafkaPort <- nextPort.toManaged
      zkPort <- nextPort .toManaged
      gM <- ZManaged.fromZIO(ZIO.environment[GreyhoundMetrics])
      kafka    <- ZManaged.acquireReleaseWith(ManagedKafka.make(ManagedKafkaConfig(kafkaPort, zkPort, Map.empty)).provideEnvironment(gM ++ ZEnvironment(Scope.global)))(_.shutdown.ignore)
      producer <- ZManaged.acquireReleaseWith(Producer.makeR[Any](ProducerConfig(kafka.bootstrapServers)).provide(ZLayer.succeed(Scope.global)))(_.shutdown)
    } yield TestResources(kafka, ReportingProducer(producer))
  }

  def clientId              = randomId.map(id => s"greyhound-consumers-$id")
  def clientId(tag: String) = randomId.map(id => s"client-$tag-$id")

  val partitions = 4
  val delete     = CleanupPolicy.Delete(1.hour.toMillis)

  def randomAlphaLowerChar = {
    val low  = 'a'.toInt
    val high = 'z'.toInt + 1
    nextIntBetween(low, high).map(_.toChar)
  }

  val randomId    = ZIO.collectAll(List.fill(6)(randomAlphaLowerChar)).map(_.mkString)
  val randomGroup = randomId.map(id => s"group-$id")

  implicit class ManagedKafkaOps(val kafka: ManagedKafka) {
    def createRandomTopic(partitions: Int = partitions, prefix: String = "topic", params: Map[String, String] = Map.empty) = for {
      topic <- randomId.map(id => s"$prefix-$id")
      _     <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete, params))
    } yield topic
  }
}



//package com.wixpress.dst.greyhound.testenv
//
//import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetrics
//import com.wixpress.dst.greyhound.core.producer.{Producer, ProducerConfig, ReportingProducer}
//import com.wixpress.dst.greyhound.core.testkit.TestMetrics
//import com.wixpress.dst.greyhound.core.{CleanupPolicy, TopicConfig}
//import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
//import zio.{Trace, ZIO, ZLayer, ZManaged, durationInt, random, test}
//import zio.managed._
//
//object ITEnv {
//  implicit val trace = Trace.empty
//  type Env = TestMetrics
//  case class TestResources(kafka: ManagedKafka, producer: ReportingProducer[Any])
//
//  def ManagedEnv: UManaged[TestMetrics] =
//    (ZLayer.succeed(TestMetrics.make) ++ ZLayer.succeed(GreyhoundMetrics.live)).build.map(_.)
////    for {
////      testMetrics <-
////      env         = (GreyhoundMetrics.live)
////    } yield env ++ testMetrics
//
//  def testResources(): ZManaged[TestMetrics, Throwable, TestResources] = {
//    for {
//      kafka    <- ManagedKafka.make(ManagedKafkaConfig.Default)
//      producer <- Producer.makeR[Any](ProducerConfig(kafka.bootstrapServers))
//    } yield TestResources(kafka, ReportingProducer(producer))
//  }
//
//  def clientId              = randomId.map(id => s"greyhound-consumers-$id")
//  def clientId(tag: String) = randomId.map(id => s"client-$tag-$id")
//
//  val partitions = 4
//  val delete     = CleanupPolicy.Delete(1.hour.toMillis)
//
//  def randomAlphaLowerChar = {
//    val low  = 'a'.toInt
//    val high = 'z'.toInt + 1
//    random.nextIntBetween(low, high).map(_.toChar)
//  }
//
//  val randomId    = ZIO.collectAll(List.fill(6)(randomAlphaLowerChar)).map(_.mkString)
//  val randomGroup = randomId.map(id => s"group-$id")
//
//  implicit class ManagedKafkaOps(val kafka: ManagedKafka) {
//    def createRandomTopic(partitions: Int = partitions, prefix: String = "topic", params: Map[String, String] = Map.empty) = for {
//      topic <- randomId.map(id => s"$prefix-$id")
//      _     <- kafka.createTopic(TopicConfig(topic, partitions, 1, delete, params))
//    } yield topic
//  }
//}
