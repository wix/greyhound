# Greyhound 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.wix/greyhound-core_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.wix/greyhound-core_2.12) [![Build Status](https://api.cirrus-ci.com/github/wix/greyhound.svg)](https://cirrus-ci.com/github/wix/greyhound/master)

Scala/Java High-level SDK for [Apache Kafka](https://kafka.apache.org/).

![Greyhound](./docs/logo.png)

## Why Greyhound?

Kafka is shipped with a Java SDK which allows developers to interact with a Kafka cluster.
However, this SDK consists of somewhat low-level APIs which are difficult to use correctly.
Greyhound seeks to provide a higher-level interface to Kafka and to express richer
semantics such as parallel message handling or retry policies with ease.
<br><br>
*The Open-sourced version of Greyhound is still in stages of initial rollout, so APIs might not be fully stable yet.*

### Available APIs:
 * ZIO based API
 * Scala Futures
 * Java.

### Greyhound main features:

 * **Declarative API** - when you want to consume messages from Kafka using the consumer API
   provided in the Java SDK, you need to run an infinite while loop which polls for new records,
   execute some custom code, and commit offsets. This might be fine for simple applications, however, it's hard
   to get all the subtleties right - especially when you want to ensure your processing guarantees
   (when do you commit), handle consumer rebalances gracefully, handle errors (Either originating from Kafka or from user's code), etc.
   This requires specific know-how and adds a lot of boilerplate when all you want to do is process messages from a topic.
   Greyhound tries to abstract away these complexities by providing a simple, declarative API, and
   to allow the developers to focus on their business logic instead of how to access Kafka correctly.

 * **Parallel message handling** - A single Kafka consumer is single-threaded, and if you want to
   achieve parallelism with your message handling (which might be crucial for high throughput
   topics) you need to manually manage your threads and/or deploy more consumer instances.
   Greyhound automatically handles parallelizing message handling for you with automatic throttling.
   Also, Greyhound uses a concurrency model based on fibers (or green-threads) which are much more
   lightweight than JVM threads, and makes async workloads extremely efficient.

 * **Consumer retries** - error handling is tricky. Sometimes things fail without our control
   (database is temporarily down, API limit exceeded, network call timed-out, etc.) and the only
   thing we can do to recover is to retry the same operation after some back-off. However, we do not
   want to block our consumer until the back-off expires, nor do we want to retry the action in a
   different thread and risk losing the messages in case our process goes down. Greyhound provides
   a robust retry mechanism, which produces failed records to special retry topics where they will be
   handled later, allowing the main consumer to keep working while ensuring no messages will be lost.

 * **Observability** - Greyhound reports many useful metrics which are invaluable when trying to
   debug your system, or understand how it is operating.

## Support

For any questions or comments open an issue or join **#greyhound** channel on our [Wix.com Slack workspace](https://join.slack.com/t/wix-oss/shared_invite/zt-dmz06ume-eMI3cd93NtGRuMwjaBO0lg)

## Usage

### Add greyhound to your build
All Greyhound modules can be found in [Maven Central Repository](https://search.maven.org/search?q=greyhound).

See [examples](./docs/build.md) of how to add greyhound modules to your build (Maven, Gradle, SBT, etc...).

### Basics

First let's review some basic messaging terminology:

 * Kafka maintains feeds of messages in categories called topics.
 * Processes that publish messages to a Kafka topic are called producers.
 * Processes that subscribe to topics and process the feed of published messages are called consumers.
 * Kafka is run as a cluster comprised of one or more servers, each of which is called a broker.

### Scala `Future` based API

The basic Future API is less powerful than the [ZIO API](#zio-api), but it's a quick way to get started
without prior knowledge of effect systems.  

```scala
import com.wixpress.dst.greyhound.core.consumer.ConsumerRecord
import com.wixpress.dst.greyhound.core.producer.ProducerRecord
import com.wixpress.dst.greyhound.core.Serdes
import com.wixpress.dst.greyhound.future._
import com.wixpress.dst.greyhound.future.GreyhoundConsumer.aRecordHandler
import scala.concurrent.{Future, ExecutionContext}

val config = GreyhoundConfig(Set("localhost:9092"))

// Define your Greyhound topology
val builder = GreyhoundConsumersBuilder(config)
  .withConsumer(
    GreyhoundConsumer(
      topic = "some-topic",
      group = "some-consumer-group",
      handle = aRecordHandler {
        new RecordHandler[Int, String] {
          override def handle(record: ConsumerRecord[Int, String])(implicit ec: ExecutionContext): Future[Any] =
            Future{
              /* Your handling logic */
            }
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
    // Report to Prometheus / StatsD / OpenTracing etc..
  }
  .build

val config  = GreyhoundConfig(Set("boostrap-server"), runtime)
val builder = GreyhoundConsumersBuilder(config)
                  .withConsumer(..)
  // ...
```

### Java API

Greyhound also offers a Java API - example usage can be found in the
[tests](./java-interop/src/test/java/com/wixpress/dst/greyhound/java/GreyhoundBuilderTest.java). 

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

val bootstrapServer = "localhost:9092"
val config = ProducerConfig(Set(bootstrapServer)/*, retryPolicy, extraProperties*/)

Producer.make[Any](config).use { producer =>
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

To consume a topic from Kafka, We'll create a `RecordConsumer` by providing the consumer group and set of
topics to subscribe to. We'll attach a `RecordHandler` to have our custom user code executed upon every
record, and choose (or implement a custom) `Deserializer` to transform the byte arrays to strongly typed values.

##### RecordConsumer 

Start a consumer by providing a Consumer Group ID, a set of topics to subscribe to and the [RecordHandler](#record-handler) to
execute custom code upon new individual records.<br>
Ordering in Kafka is only guaranteed within a single partition, so Greyhound will parallelize execution 
by devoting a single fiber for each partition. It will also automatically
pause polling for specific partitions in case handling is too slow, without affecting other partitions.

```scala
import com.wixpress.dst.greyhound.core.consumer._
import zio._

val group = "some-consumer-group-id"
val handler: RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]] = ???

// Start consumer, will close on interruption
val bootstrapServers = Set("localhost:9092")
val initialTopics = Set("topic-A")
RecordConsumer.make(RecordConsumerConfig(bootstrapServers, group, initialTopics), handler).useForever
```

##### Record handler

A `RecordHandler[-R, +E, K, V]` describes a handling function on one or more topics. It handles records
of type `ConsumerRecord[K, V]`, requires an environment of type `R` and might fail with errors of type `E`.

The `RecordHandler` is a composable building block, which means you can provide middleware record handlers
that can intercept and enrich custom code without any magic involved.<br>
You can transform your handler by using the built-in combinators like so (types can be inferred, but shown here for readability):

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
  handler1.withErrorHandler { case (error, record) =>
    putStrLn(error.getMessage)
  }

// Deserialize records
val emailIdDeserializer: Deserializer[EmailId] = Serdes.IntSerde.map(EmailId)
val emailRequestDeserializer: Deserializer[EmailRequest] = ???
val handler3: RecordHandler[Console, Nothing, Chunk[Byte], Chunk[Byte]] =
  handler2.withDeserializers(emailIdDeserializer, emailRequestDeserializer)

```

Notice that `RecordConsumer` accepts a `RecordHandler[_, _, Chunk[Byte], Chunk[Byte]]`, indicating that
key and value deserializers to `Chunk[Byte]` must be applied to any handler the user provides.<br>
You can either write a handler that accepts Chu  

### Serializers / deserializers

Kafka doesn't know or care about your message formats when producing or consuming. The underlying
protocol uses raw byte arrays to represent your messages, so it's your job to tell it how to serialize
and deserialize your domain objects and custom types to/from bytes.

 * `Serializer[-A]` - takes values of type `A` and converts them to bytes
 * `Deserializer[+A]` - takes bytes and converts them to values of type `A`
 * `Serde[A]` is both a `Serializer[A]` and a `Deserializer[A]`

#### Transforming

Often serialization / deserialization could be created by modifying existing data types. For example,
you could encode a timestamp as a `Long` if you use the epoch millis as the representation. You can
use the built in combinators to transform existing deserializers:

```scala
import com.wixpress.dst.greyhound.core.{Serdes, Deserializer}
import java.time.Instant

val longDeserializer: Deserializer[Long] = Serdes.LongSerde

val instantDeserializer: Deserializer[Instant] =
  longDeserializer.map(millis => Instant.ofEpochMilli(millis))
```

You could also modify a serializer by adapting the input using the `contramap` combinator:

```scala
import com.wixpress.dst.greyhound.core.{Serdes, Serializer}
import java.time.Instant

val longSerializer: Serializer[Long] = Serdes.LongSerde

val instantSerializer: Serializer[Instant] =
  longSerializer.contramap(instant => instant.toEpochMilli)
```

Or do both simultaneously using the `inmap` combinator:

```scala
import com.wixpress.dst.greyhound.core.{Serdes, Serde}
import java.time.Instant

val longSerde: Serde[Long] = Serdes.LongSerde

val instantSerde: Serde[Instant] =
  longSerde.inmap(Instant.ofEpochMilli)(_.toEpochMilli)
```

This could be useful for your own custom domain types as well. For example, modifying a string
or byte array `Serde` to represent your own types encoded as JSON.

#### Consumer Retries

`RecordHandler` provides a built-in retry mechanism for consumer code. It is possible to create a retry policy for failed
user-supplied effects. The retry mechanism is influenced by this [Uber's blog post](https://blog.pragmatists.com/retrying-consumer-architecture-in-the-apache-kafka-939ac4cb851a).<br>
A retry policy is defined by a sequence of intervals indicating the back-off time between attemps. For each attempt
Greyhound automatically creates a topic named: `$original_topic-$group_id-retry-[0..n]` and subscribes to it.<br>
When an effect fails, Greyhound either submits the record to the subsequent retry topic, adding specific headers indicating when to execute the handler for this record.<br>
When the record is consumed via the retry topics, the record handler reads the relevant headers and potentially 'sleeps' until it is time to invoke the user code.<br>
Notice this waiting is done in a non-blocking way, so no resources are wasted.

Usage:
```scala
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.producer._

val handler = RecordHandler(topic) { record: ConsumerRecord[String, String] =>
  if (record.value === "OK")
    ZIO.unit
      else 
    ZIO.fail(new RuntimeException("Failed..."))     
}

Producer.make[Any](producerConfig).use { producer =>
    val retryPolicy = RetryPolicy.default("groupId", 1.second, 30.seconds, 1.minute)
    val retryHandler = handler
      .withDeserializers(StringSerde, StringSerde)
      .withRetries(retryPolicy, producer)
      .ignore
}
```

In this example the record handler fails for any input other than the string "OK", so any other record will be re-sent to the subsequent topic,
until finally the record is consumed by last topic.  

### Testing
Use the embedded Kafka to test your app:
```scala
ManagedKafka.make(ManagedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)).use { kafka =>
  // Start producing and consuming messages,
  // configure broker address on producers and consumers to localhost:9092


  // outside of this scope Kafka will be shutdown.
}
```

This will start a real Kafka broker and Zookeeper instance on defined ports. Use those ports to access Kafka within the managed
scope.