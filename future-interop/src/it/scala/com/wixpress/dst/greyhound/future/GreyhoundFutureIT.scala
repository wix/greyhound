package com.wixpress.dst.greyhound.future

import java.util.concurrent.atomic.AtomicInteger

import com.wixpress.dst.greyhound.core._
import com.wixpress.dst.greyhound.core.admin.AdminClientConfig
import com.wixpress.dst.greyhound.core.consumer.EventLoopMetric.{StartingEventLoop, StoppingEventLoop}
import com.wixpress.dst.greyhound.core.consumer.OffsetReset
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.testkit.RecordMatchers._
import com.wixpress.dst.greyhound.future.GreyhoundFutureIT._
import com.wixpress.dst.greyhound.future.ContextDecoder.aHeaderContextDecoder
import com.wixpress.dst.greyhound.future.ContextEncoder.aHeaderContextEncoder
import com.wixpress.dst.greyhound.future.ErrorHandler.anErrorHandler
import com.wixpress.dst.greyhound.future.GreyhoundConsumer._
import com.wixpress.dst.greyhound.testkit.{ManagedKafka, ManagedKafkaConfig}
import org.specs2.concurrent.ExecutionEnv
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.{AfterAll, BeforeAll, Scope}
import zio.{Task, UIO, URIO, Promise => ZPromise}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Random, Try}

class GreyhoundFutureIT(implicit ee: ExecutionEnv)
  extends SpecificationWithJUnit
    with BeforeAll
    with AfterAll {

  private var environment: Environment = _

  override def beforeAll(): Unit =
    environment = runtime.unsafeRun(Environment.make)

  override def afterAll(): Unit =
    runtime.unsafeRun(environment.shutdown)

  "produce and consume a single message" in new Ctx {
    val promise = Promise[ConsumerRecord[Int, String]]
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val consumersBuilder = GreyhoundConsumersBuilder(config)
      .withConsumer(
        GreyhoundConsumer(
          initialTopics = Set(topic),
          group = "group-1",
          clientId = "client-id-1",
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

  trait RetryContext extends Ctx {
    val promise = Promise[ConsumerRecord[Int, String]]
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val successfulAttempt = 3
    val atomicInteger = new AtomicInteger(successfulAttempt)
    val consumer = GreyhoundConsumer(
      initialTopics = Set(topic),
      group = "group-1",
      clientId = "client-id-1",
      handle = aRecordHandler {
        new RecordHandler[Int, String] {
          override def handle(record: ConsumerRecord[Int, String])(implicit ec: ExecutionContext): Future[Any] =
            atomicInteger.decrementAndGet() match {
              case i if i > 0 => Future.failed(new RuntimeException("Oops"))
              case _ => Future.successful(promise.success(record.copy(value = "Promise fulfilled")))
            }
        }
      },
      keyDeserializer = Serdes.IntSerde,
      valueDeserializer = Serdes.StringSerde)
  }

  "consume a message with nonblocking retry policy" in new RetryContext {
    val result = for {
      consumers <- GreyhoundConsumersBuilder(config)
        .withConsumer(consumer.withNonBlockingRetry(5.millis, 5.millis))
        .build
      producer <- GreyhoundProducerBuilder(config).build
      _ <- producer.produce(
        record = ProducerRecord(topic, "Promise!", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
      promiseResult <- promise.future
      _ <- producer.shutdown
      _ <- consumers.shutdown
    } yield promiseResult must(
      beRecordWithKey(123) and
        beRecordWithValue("Promise fulfilled"))

    result.awaitFor(1.minute)
  }

  "consume a message with blocking retry policy" in new RetryContext {
    val result = for {
      consumers <- GreyhoundConsumersBuilder(config)
        .withConsumer(consumer.withBlockingRetry(5.millis, 5.millis))
        .build
      producer <- GreyhoundProducerBuilder(config).build
      _ <- producer.produce(
        record = ProducerRecord(topic, "Promise!", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
      promiseResult <- promise.future
      _ <- producer.shutdown
      _ <- consumers.shutdown
    } yield promiseResult must(
      beRecordWithKey(123) and
        beRecordWithValue("Promise fulfilled"))

    result.awaitFor(1.minute)
  }

  "propagate context from producer to consumer" in new Ctx {
    implicit val context = Context("some-context")

    val promise = Promise[Context]
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val consumersBuilder = GreyhoundConsumersBuilder(config)
      .withConsumer(
        GreyhoundConsumer(
          initialTopics = Set(topic),
          group = "group-2",
          clientId = "client-id-0",
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

  "collect metrics with custom reporter" in new Ctx {
    val metrics = ListBuffer.empty[GreyhoundMetric]
    val runtime = GreyhoundRuntimeBuilder()
      .withMetricsReporter(metric => UIO(metrics += metric))
      .build
    val config = GreyhoundConfig(environment.kafka.bootstrapServers, runtime)
    val builder = GreyhoundConsumersBuilder(config)
      .withConsumer(
        GreyhoundConsumer(
          initialTopics = Set(topic),
          group = "group-3",
          clientId = "client-id-3",
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
      (contain[GreyhoundMetric](StartingEventLoop("client-id-3", "group-3")) and
        contain[GreyhoundMetric](StoppingEventLoop("client-id-3", "group-3"))).awaitFor(1.minute)
  }

  "handle errors" in new Ctx {
    val errorRecord = Promise[ConsumerRecord[Int, String]]
    val errorMessage = Promise[String]
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val consumersBuilder = GreyhoundConsumersBuilder(config)
      .withConsumer(
        GreyhoundConsumer(
          initialTopics = Set(topic),
          group = "group-2",
          clientId = "client-id-2",
          handle = aRecordHandler {
            new RecordHandler[Partition, String] {
              override def handle(record: ConsumerRecord[Partition, String])(implicit ec: ExecutionContext): Future[Any] =
                Future.failed(new RuntimeException("Expected_Error"))
            }
          },
          keyDeserializer = Serdes.IntSerde,
          valueDeserializer = Serdes.StringSerde,
          errorHandler = anErrorHandler[Int, String]((e, record) => Future.successful {
            errorMessage.trySuccess(e.getLocalizedMessage)
            errorRecord.trySuccess(record)
          })
        ))

    val (err, recordOnErrorHandler) = Await.result(for {
      consumers <- consumersBuilder.build
      producer <- GreyhoundProducerBuilder(config).build
      _ <- producer.produce(
        record = ProducerRecord(topic, "hello world", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
      errorRecord <- errorRecord.future
      errorMessage <- errorMessage.future
      _ <- producer.shutdown
      _ <- consumers.shutdown
    } yield (errorMessage, errorRecord)
      , 20.seconds)

    recordOnErrorHandler must (beRecordWithKey(123) and beRecordWithValue("hello world"))
    err === "Expected_Error"
  }

  "override consumer properties" in new Ctx {
    val handlerInvocations = new AtomicInteger(0)
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val accumulator: Handle[Partition, String] = aRecordHandler {
      new RecordHandler[Partition, String] {
        override def handle(record: ConsumerRecord[Partition, String])(implicit ec: ExecutionContext): Future[Any] =
          Future.successful(handlerInvocations.incrementAndGet)
      }
    }

    val consumer = GreyhoundConsumer(
      initialTopics = Set(topic),
      group = "groupXXX",
      clientId = "clientIdX",
      handle = accumulator,
      offsetReset = OffsetReset.Earliest,
      keyDeserializer = Serdes.IntSerde,
      valueDeserializer = Serdes.StringSerde)

    val sameSpecWithOverridenGroupInProperties =
      consumer
        .copy(clientId = "clientIdY")
        .withConsumerMutate(_.copy(extraProperties = Map("group.id" -> "different-group")))

    val consumersBuilder = GreyhoundConsumersBuilder(config)
      .withConsumer(consumer)
      .withConsumer(sameSpecWithOverridenGroupInProperties)

    val (consumers, producer) = Await.result(for {
      consumers <- consumersBuilder.build
      producer <- GreyhoundProducerBuilder(config).build
      _ <- producer.produce(
        record = ProducerRecord(topic, "hello world", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
    } yield (consumers, producer), 60.seconds)

    /*Since it's 2 consumers with different groups, handler is invoked twice */
    eventually(handlerInvocations.get === 2)

    consumers.shutdown
    producer.shutdown
    ok
  }

  "override producer properties" in new Ctx {
    val config = GreyhoundConfig(environment.kafka.bootstrapServers)
    val produceFail = Try(Await.result(for {
      producer <- GreyhoundProducerBuilder(config,
        mutateProducer = _.withProperties(Map("max.request.size" -> "1"))).build
      _ <- producer.produce(
        record = ProducerRecord(topic, "hello world", Some(123)),
        keySerializer = Serdes.IntSerde,
        valueSerializer = Serdes.StringSerde)
    } yield (), 60.seconds))


    produceFail.failed.get.getCause.getClass.getSimpleName === "RecordTooLargeException"
  }


  class Ctx extends Scope {
    val topic: Topic = s"some-topic-${Random.alphanumeric.take(5).mkString}"
    AdminClient.create(AdminClientConfig(environment.kafka.bootstrapServers)).createTopic(
      TopicConfig(
        name = topic,
        partitions = 4,
        replicationFactor = 1,
        cleanupPolicy = CleanupPolicy.Delete(1.hour.toMillis)))
  }

}

object GreyhoundFutureIT {
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
    }.forkDaemon
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
