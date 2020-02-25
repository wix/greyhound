# Greyhound 🐕 [![Build Status](https://travis-ci.com/wix-incubator/greyhound.svg?branch=master)](https://travis-ci.com/wix-incubator/greyhound)

Opinionated SDK for [Apache Kafka](https://kafka.apache.org/)

## Why Greyhound?

Kafka is shipped with a Java SDK which allows developers to interact with a Kafka cluster.
However, this SDK consists of somewhat low-level APIs which are difficult to use correctly.
Greyhound seeks to provide a higher-level interface to Kafka and to express richer
semantics such as parallel message handling or retry policies with ease.

### Notable features Greyhound provides:

 * **Declarative API** - when you want to consume messages from Kafka using the consumer API
   provided in the Java SDK, you need to run an infinite while loop which polls new records,
   handle them and commit offsets. This might be fine for simple applications, however it's hard
   to get all the subtleties right - especially when you want to ensure your processing guarantees
   (when do you commit), handle consumer rebalance, handle exceptions, etc. This requires specific
   know-how and adds a lot of boilerplate when all you want to do is process messages from a topic.
   Greyhound tries to abstract away these complexities by providing a simple, declarative API, and
   to allow the developers to focus on their business logic instead of how to operate Kafka correctly.
   
 * **Parallel message handling** - A single Kafka consumer is single-threaded, and if you want to
   achieve parallelism with your message handling (which might be crucial for high throughput
   topics) you need to manually manage your threads. Greyhound automatically handles parallelising
   message handling for you with automatic throttling. Also, Greyhound uses a concurrency model
   based on fibers (or green-threads) which are much more lightweight than JVM threads,
   and makes async workloads extremely efficient.
   
 * **Consumer retries** - error handling is tricky. Sometimes things fail without our control
   (database is temporarily down, API limit exceeded, network call timed-out, etc.) and the only
   thing we can do to recover is to retry the same operation after some backoff. However, we do not
   want to block our consumer until the backoff expires, nor do we want to retry the action in a
   different thread and risk losing the messages in case our process goes down. Greyhound provides
   a robust retry mechanism, which produces failed records to special retry topics where they will be
   handled later, allowing the main consumer to keep working while ensuring no messages will be lost.
 
 * **Observability** - Greyhound reports many useful metrics which are invaluable when trying to
   debug your system, or understand how it is operating.
 
 * Context propagation - _TODO_
 * Safety - _TODO_

## Usage

### Basics

First let's review some basic messaging terminology:

 * Kafka maintains feeds of messages in categories called topics.
 * We'll call processes that publish messages to a Kafka topic producers.
 * We'll call processes that subscribe to topics and process the feed of published messages consumers.
 * Kafka is run as a cluster comprised of one or more servers each of which is called a broker.

### Serializers / deserializers

Kafka doesn't know or care about your message formats when producing or consuming. The underlying
protocol uses raw byte arrays to represent your messages, so it's your job to tell it how to serialize
and deserialize your domain objects and custom types to/from bytes.

 * `Serializer[-A]` - takes values of type `A` and converts them to bytes
 * `Deserializer[+A]` - takes bytes and converts them to values of type `A`
 * `Serde[A]` is both a `Serializer[A]` and a `Deserializer[A]`

### Future API

```scala
import com.wixpress.dst.greyhound.core.consumer.ConsumerRecord
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.future._
import scala.concurrent.{Future, ExecutionContext}

val bootstrapServer = "localhost:6667"
val config = GreyhoundConfig(Set(bootstrapServer))

// Define your Greyhound topology
val builder = GreyhoundConsumersBuilder(config)
  .withConsumer(
    GreyhoundConsumer(
      topic = "some-topic",
      group = "some-group",
      handler = new RecordHandler[Int, String] {
        override def handle(record: ConsumerRecord[Int, String])(implicit ec: ExecutionContext): Future[Any] =
          Future {
            // Your handling logic
          }
      },
      keyDeserializer = Serdes.IntSerde,
      valueDeserializer = Serdes.StringSerde))

for {
  // Start consuming
  consumers <- builder.build
  
  // Create a producer and produce to topic
  producer <- GreyhoundProducerBuilder(config).build
  _ <- producer.produce(
    record = ProducerRecord("some-topic", "hello world", Some(123)),
    keySerializer = Serdes.IntSerde,
    valueSerializer = Serdes.StringSerde)

  // Shutdown all consumers and producers
  _ <- producer.shutdown
  _ <- consumers.shutdown
} yield ()
```

#### Using a custom metrics reporter

By default, all Greyhound metrics are reported using a simple [SLF4J](http://www.slf4j.org/) logger.
You can easily swap it for your own custom reporter like so:

```scala
import com.wixpress.dst.greyhound.core.metrics.GreyhoundMetric
import com.wixpress.dst.greyhound.future._

val runtime = GreyhoundRuntimeBuilder()
  .withMetricsReporter { metric: GreyhoundMetric =>
    // Report to Prometheus / StatsD / whatever
  }
  .build

val config = GreyhoundConfig(???, runtime)
val builder = GreyhoundConsumersBuilder(???)
  // ...
```

### ZIO API

Greyhound is based on the [ZIO](https://zio.dev/) library which provides type-safe,
composable asynchronous and concurrent programming environment for Scala. These docs
assume the reader has basic familiarity with ZIO's core notions, like effects, managed
resources etc.

#### Producing messages

In order to produce a messages to Kafka, you need a producer. `Producer.make` will create a
producer wrapped in a [`ZManaged`](https://zio.dev/docs/datatypes/datatypes_managed).
You can then use it like so:

```scala
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.Serdes

val bootstrapServer = "localhost:6667"
val config = ProducerConfig(Set(bootstrapServer))

Producer.make(config).use { producer =>
  producer.produce(
    record = ProducerRecord(
      topic = "some-topic",
      value = "hello world",
      key = Some(1234)),
    keySerializer = Serdes.IntSerde,
    valueSerializer = Serdes.StringSerde)
}
``` 

#### Consuming messages

##### Record handler

A `RecordHandler[-R, +E, K, V]` describes a handling function on one or more topics. It handles records
of type `ConsumerRecord[K, V]`, requires an environment of type `R` and might fail with errors of type `E`.

You can transform your handler by using the built-in combinators:

```scala
import java.util.UUID
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.Deserializer
import com.wixpress.dst.greyhound.core.Serdes
import zio.console._
import zio._

case class EmailId(id: Int)
case class EmailRequest(/*...*/)

// Base handler with your custom logic
val emailRequestsTopic = "email-requests"
val handler1: RecordHandler[Any, RuntimeException, EmailId, EmailRequest] =
  RecordHandler(emailRequestsTopic) { record =>
    // Do something with email requests...
    ZIO.fail(new RuntimeException("Oops!"))
  }

// Recover from errors
val handler2: RecordHandler[Console, Nothing, EmailId, EmailRequest] =
  handler1.withErrorHandler { error =>
    putStrLn(error.getMessage)
  }

// Deserialize records
val emailIdDeserializer: Deserializer[EmailId] = Serdes.IntSerde.map(EmailId)
val emailRequestDeserializer: Deserializer[EmailRequest] = ???
val handler3: RecordHandler[Console, Nothing, Chunk[Byte], Chunk[Byte]] =
  handler2.withDeserializers(emailIdDeserializer, emailRequestDeserializer)

// Combine multiple handlers
val otherHandler: RecordHandler[Console, Nothing, Chunk[Byte], Chunk[Byte]] = ???
val handler4 = handler3 combine otherHandler
```

##### Parallel consumer 

Once you define your handler, you can start consuming from a Kafka topic. You can
create a `ParallelConsumer` with subscribes to one or more consumer groups and will start
handling records from configured topics by the handlers.

Since ordering in Kafka is only guaranteed within a single partition, the parallel consumer
will parallelize handling by devoting a single fiber for each partition. It will also automatically
throttle specific partitions in case handling is too slow, without affecting other partitions.

```scala
import com.wixpress.dst.greyhound.core.consumer._
import zio._

val group = "some-consumer-group"
val handler: RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]] = ???

// Start consumer, will close on interruption
val bootstrapServers = Set("localhost:6667")
ParallelConsumer.make(bootstrapServers, group -> handler).useForever
```
