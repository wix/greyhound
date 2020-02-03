# Greyhound 🐕

Opinionated SDK for [Apache Kafka](https://kafka.apache.org/)

## Why Greyhound?

Kafka is shipped with a Java SDK which allows developers to interact with a Kafka cluster.
However, this SDK consists of somewhat low-level APIs which are difficult to use correctly.
Greyhound seeks to provide a higher-level interface to Kafka and to express richer
semantics such as parallel message handling or retry policies with ease.

### Notable features Greyhound provides:

 * Declarative API - _TODO_
 * Parallel message handling - _TODO_
 * Context propagation - _TODO_
 * Retries - _TODO_
 * Safety - _TODO_
 * Observability - _TODO_

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

### Producing messages

In order to produce a messages to Kafka, you need a producer. `Producer.make` will create a
producer wrapped in a `ZManaged`. You can than use it like so:

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

### Consuming messages

#### Record handler

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

#### Parallel consumer 

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
