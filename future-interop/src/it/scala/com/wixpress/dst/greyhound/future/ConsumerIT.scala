package com.wixpress.dst.greyhound.future

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.consumer.ConsumerRecord
import com.wixpress.dst.greyhound.core.consumer.EventLoopMetric.{StartingEventLoop, StoppingEventLoop}
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.future.ConsumerIT._
import com.wixpress.dst.greyhound.future.ContextDecoder.aHeaderContextDecoder
import com.wixpress.dst.greyhound.future.ContextEncoder.aHeaderContextEncoder
import com.wixpress.dst.greyhound.future.GreyhoundConsumer._
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
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val consumersBuilder = GreyhoundConsumersBuilder(config)
      .withConsumer(
        GreyhoundConsumer(
          topic = topic,
          group = "group-1",
          handle = aRecordHandler {
            new RecordHandler[Int, String] {
              override def handle(record: ConsumerRecord[Int, String])(implicit ec: ExecutionContext): Future[Any] =
                Future.successful(promise.success(record))
            }
          },
          keyDeserializer = Serdes.IntSerde,
          valueDeserializer = Serdes.StringSerde))

    val handled = for {
      consumers <- consumersBuilder.build
      producer <- GreyhoundProducerBuilder(config).build
      _ <- producer.produce(
        record = ProducerRecord(topic, "hello world", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
      handled <- promise.future
      _ <- producer.shutdown
      _ <- consumers.shutdown
    } yield handled

    handled must (beRecordWithKey(123) and beRecordWithValue("hello world")).awaitFor(1.minute)
  }

  "propagate context from producer to consumer" in {
    implicit val context = Context("some-context")

    val promise = Promise[Context]
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val consumersBuilder = GreyhoundConsumersBuilder(config)
      .withConsumer(
        GreyhoundConsumer(
          topic = topic,
          group = "group-2",
          handle = aContextAwareRecordHandler(Context.Decoder) {
            new ContextAwareRecordHandler[Int, String, Context] {
              override def handle(record: ConsumerRecord[Int, String])(implicit context: Context, ec: ExecutionContext): Future[Any] =
                Future.successful {
                  if (context != Context.Empty) {
                    promise.success(context)
                  }
                }
            }
          },
          keyDeserializer = Serdes.IntSerde,
          valueDeserializer = Serdes.StringSerde))

    val producerBuilder = GreyhoundProducerBuilder(config)
      .withContextEncoding(Context.Encoder)

    val handled = for {
      consumers <- consumersBuilder.build
      producer <- producerBuilder.build
      _ <- producer.produce(
        record = ProducerRecord(topic, "hello world", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
      handled <- promise.future
      _ <- producer.shutdown
      _ <- consumers.shutdown
    } yield handled

    handled must equalTo(context).awaitFor(1.minute)
  }

  "collect metrics with custom reporter" in {
    val metrics = ListBuffer.empty[GreyhoundMetric]
    val runtime = GreyhoundRuntimeBuilder()
      .withMetricsReporter(metric => metrics += metric)
      .build
    val config = GreyhoundConfig(environment.kafka.bootstrapServers, runtime)
    val builder = GreyhoundConsumersBuilder(config)
      .withConsumer(
        GreyhoundConsumer(
          topic = topic,
          group = "group-3",
          handle = aRecordHandler {
            new RecordHandler[Int, String] {
              override def handle(record: ConsumerRecord[Int, String])(implicit ec: ExecutionContext): Future[Any] =
                Future.unit
            }
          },
          keyDeserializer = Serdes.IntSerde,
          valueDeserializer = Serdes.StringSerde))

    val recordedMetrics = for {
      consumers <- builder.build
      _ <- consumers.shutdown
    } yield metrics.toList

    recordedMetrics must
      (contain[GreyhoundMetric](StartingEventLoop) and
        contain[GreyhoundMetric](StoppingEventLoop)).awaitFor(1.minute)
  }

}

object ConsumerIT {
  val topic: Topic = "some-topic"
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

case class Context(value: String)

object Context {
  private val header = "context"
  private val serde = Serdes.StringSerde.inmap(Context(_))(_.value)

  val Empty = Context("")
  val Encoder = aHeaderContextEncoder(header, serde)
  val Decoder = aHeaderContextDecoder(header, serde, Empty)
}
