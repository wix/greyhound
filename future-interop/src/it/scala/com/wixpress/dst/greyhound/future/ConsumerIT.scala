package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.ConsumerRecord
import com.wixpress.dst.greyhound.core.consumer.EventLoopMetric.{StartingEventLoop, StoppingEventLoop}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.future.ConsumerIT._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import org.specs2.concurrent.ExecutionEnv
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.{AfterAll, BeforeAll}
import zio.duration.{Duration => ZDuration}
import zio.{Task, URIO, Promise => ZPromise}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

class ConsumerIT(implicit ee: ExecutionEnv)
  extends SpecificationWithJUnit
    with BeforeAll
    with AfterAll {

  private var environment: Environment = _

  override def beforeAll(): Unit =
    environment = runtime.unsafeRun {
      Environment.make.tap { environment =>
        environment.kafka.createTopic(
          TopicConfig(
            name = topic,
            partitions = 8,
            replicationFactor = 1,
            cleanupPolicy = CleanupPolicy.Delete(ZDuration.fromScala(1.hour))))
      }
    }

  override def afterAll(): Unit =
    runtime.unsafeRun(environment.shutdown)

  "produce and consume a single message" in {
    val promise = Promise[ConsumerRecord[Int, String]]
    val builder = GreyhoundBuilder(GreyhoundConfig(environment.kafka.bootstrapServers))
      .withConsumer(
        GreyhoundConsumer(
          topic = topic,
          group = group,
          handler = new RecordHandler[Int, String] {
            override def handle(record: ConsumerRecord[Int, String])(implicit ec: ExecutionContext): Future[Any] =
              Future.successful(promise.success(record))
          },
          keyDeserializer = Serdes.IntSerde,
          valueDeserializer = Serdes.StringSerde))

    val handled = for {
      greyhound <- builder.build
      producer <- greyhound.producer(GreyhoundProducerConfig())
      _ <- producer.produce(
        record = ProducerRecord(topic, "hello world", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
      handled <- promise.future
      _ <- greyhound.shutdown
    } yield handled

    handled must (beRecordWithKey(123) and beRecordWithValue("hello world")).awaitFor(1.minute)
  }

  "collect metrics with custom reporter" in {
    val metrics = ListBuffer.empty[GreyhoundMetric]
    val builder = GreyhoundBuilder(GreyhoundConfig(environment.kafka.bootstrapServers))
      .withConsumer(
        GreyhoundConsumer(
          topic = topic,
          group = group,
          handler = new RecordHandler[Int, String] {
            override def handle(record: ConsumerRecord[Int, String])(implicit ec: ExecutionContext): Future[Any] =
              Future.unit
          },
          keyDeserializer = Serdes.IntSerde,
          valueDeserializer = Serdes.StringSerde))
      .withMetricsReporter { metric =>
        metrics += metric
      }

    val recordedMetrics = for {
      greyhound <- builder.build
      _ <- greyhound.shutdown
    } yield metrics.toList

    recordedMetrics must
      (contain[GreyhoundMetric](StartingEventLoop) and
        contain[GreyhoundMetric](StoppingEventLoop)).awaitFor(1.minute)
  }

}

object ConsumerIT {
  val topic: Topic = "some-topic"
  val group: Group = "some-group"
  val runtime = GreyhoundRuntime.Live
}

trait Environment {
  def kafka: ManagedKafka
  def shutdown: Task[Unit]
}

object Environment {
  def make: URIO[GreyhoundRuntime.Env, Environment] = for {
    closeSignal <- ZPromise.make[Nothing, Unit]
    started <- ZPromise.make[Nothing, ManagedKafka]
    fiber <- ManagedKafka.make(ManagedKafkaConfig.Default).use { kafka =>
      started.succeed(kafka) *> closeSignal.await
    }.fork
    kafka1 <- started.await
  } yield new Environment {
    override def kafka: ManagedKafka = kafka1

    override def shutdown: Task[Unit] =
      closeSignal.succeed(()) *> fiber.join
  }
}
