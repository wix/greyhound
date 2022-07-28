# ZIO Based API

Greyhound is based on the [ZIO](https://zio.dev/) library which provides a type-safe,
composable, asynchronous and concurrent programming environment for Scala. These docs
assume the reader has basic familiarity with ZIO's core notions like effects, managed
resources, etc.

## Producing messages

In order to produce a messages to Kafka, you need a producer. `Producer.make` will create a
producer wrapped in a [`ZManaged`](https://zio.dev/docs/datatypes/datatypes_managed).
You can then use it like so:

```scala
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.Serdes

val bootstrapServer = "localhost:9092"
val config = ProducerConfig(bootstrapServer/*, retryPolicy, extraProperties*/)

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

## Consuming messages

To consume a topic from Kafka, We'll create a `RecordConsumer` by providing the consumer group and set of
topics to subscribe to. We'll attach a `RecordHandler` to have our custom user code executed upon every
record, and choose (or implement a custom) `Deserializer` to transform the byte arrays to strongly typed values.

## RecordConsumer

Start a consumer by providing a Consumer Group ID, a set of topics to subscribe to (or a pattern), and the [RecordHandler](#record-handler) to
execute custom code upon new individual records.

Ordering in Kafka is only guaranteed within a single partition, so Greyhound will parallelize execution
by devoting a single fiber for each partition. It will also automatically
pause polling for specific partitions in case handling is too slow, without affecting other partitions.

```scala
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._
import zio._

val group = "some-consumer-group-id"
val handler: RecordHandler[Any, Nothing, Chunk[Byte], Chunk[Byte]] = ???

// Start consumer, will close on interruption
val bootstrapServers = "localhost:9092"
val initialTopics = Set("topic-A")
RecordConsumer.make(RecordConsumerConfig(bootstrapServers, group, ConsumerSubscription.Topics(initialTopics)), handler)
  .useForever

// Start another consumer, this time subscribing to a topic pattern
RecordConsumer.make(RecordConsumerConfig(bootstrapServers, group, ConsumerSubscription.TopicPattern("topic.*")), handler)
  .useForever
```

## Record handler

A `RecordHandler[-R, +E, K, V]` describes a handling function on one or more topics. It handles records
of type `ConsumerRecord[K, V]`, requires an environment of type `R` and might fail with errors of type `E`.

The `RecordHandler` is a composable building block, which means you can provide middleware record handlers
that can intercept and enrich custom code without any magic involved.

You can transform your handler by using the built-in combinators like so (types can be inferred, but shown here for readability):

```scala
import java.util.UUID
import com.wixpress.dst.greyhound.core.consumer.domain.RecordHandler
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
  RecordHandler { record =>
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
You can either write a handler that accepts `Chunk[Byte]` as input for key and value, or provide deserializers to the handler
and accept typed input, according to the output of the provided deserializers.

## Serializers / Deserializers

Kafka doesn't know or care about your message formats when producing or consuming. The underlying
protocol uses raw byte arrays to represent your messages, so it's your job to tell it how to serialize
and deserialize your domain objects and custom types to/from bytes.

- `Serializer[-A]` - takes values of type `A` and converts them to bytes
- `Deserializer[+A]` - takes bytes and converts them to values of type `A`
- `Serde[A]` is both a `Serializer[A]` and a `Deserializer[A]`

## Transforming

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

## Consumer Non-Blocking Retries

`RecordConsumer` provides a built-in retry mechanism for consumer code. It is possible to create a retry policy for failed
user-supplied effects. The retry mechanism is influenced by this [Uber blog post](https://blog.pragmatists.com/retrying-consumer-architecture-in-the-apache-kafka-939ac4cb851a).

A retry policy is defined by a sequence of intervals indicating the back-off time between attemps. For each attempt
Greyhound automatically creates a topic named: `$original_topic-[group_id]-retry-[0..n]` and subscribes to it.

When an effect fails, Greyhound either submits the record to the subsequent retry topic, adding specific headers indicating when to execute the handler for this record.

When the record is consumed via the retry topics, the record handler reads the relevant headers and potentially 'sleeps' until it is time to invoke the user code.

Notice this waiting is done in a non-blocking way, so no resources are wasted.

**Usage:**

```scala
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.core.consumer.retry._
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.Serdes._
import zio.duration._
import zio.ZIO

val group = "groupId"
val topic = "topicY"
val handler = RecordHandler { record: ConsumerRecord[String, String] =>
  if (record.value == "OK")
    ZIO.unit
      else
    ZIO.fail(new RuntimeException("Failed..."))
}

val retryConfig = ZRetryConfig.nonBlockingRetry(group, 1.second, 30.seconds, 1.minute)
val bootstrapServers = "localhost:9092"
val topics = Set("topic-A")
RecordConsumer.make(
  RecordConsumerConfig(bootstrapServers, group, ConsumerSubscription.Topics(topics), retryConfig = Some(retryConfig)),
  handler.withDeserializers(StringSerde, StringSerde)
  ).useForever
```

In this example, the record handler fails for any input other than the string "OK". Any other record will be re-sent to the subsequent topic,
until finally the record is consumed by the last topic.

## Configuring retries with a pattern consumer

In terms of an API - there's no difference for configuring retries for a fixed set of topics. However, when configuring a consumer that subscribes to a pattern,
we cannot use the same strategy for retry topics naming.

This is due to the fact we don't know up front which retry topics we should subscribe to, since
the original topics we're subscribing to aren't known.
The strategy for naming retry topics in this case are: `__gh_pattern-retry-[group_id]-attempt-[0..n]`.

Notice the original topic name isn't part of the retry topics. This means 2 things:

1. We know up front which topics to create and subscribe to.
2. We cannot start separate consumers with the same group id, where each subscribes to a different pattern and performs different logic.
   - This is due to the fact that all consumers will subscribe to the same retry topics. Doing something like this will result in separate consumers 'stealing each other's retry messages.
   - It may seem like an implementation detail - but it's important to proceed with the last part in mind.

## Consumer Blocking Retries

In case the consumer needs to process messages in order, a blocking retry configuration is available.

When retry configuration is set up this way, the user-provided handler code will be retried on the **same** message according to the provided intervals.

There are several different options when configuring blocking retries:

- finiteBlockingRetry
- infiniteBlockingRetry
- exponentialBackoffBlockingRetry
- blockingFollowedByNonBlockingRetry

```scala
import com.wixpress.dst.greyhound.core.consumer._
import com.wixpress.dst.greyhound.core.consumer.domain._
import com.wixpress.dst.greyhound.core.consumer.retry._
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.Serdes._
import zio.duration._
import zio.ZIO

val group = "groupId"
val topic = "topicY"
val handler = RecordHandler { record: ConsumerRecord[String, String] =>
  if (record.value == "OK")
    ZIO.unit
      else
    ZIO.fail(new RuntimeException("Failed..."))
}

val retryConfig = ZRetryConfig.finiteBlockingRetry(1.second, 30.seconds, 1.minute)
val bootstrapServers = "localhost:9092"
val topics = Set("topic-A")
RecordConsumer.make(
  RecordConsumerConfig(bootstrapServers, group, ConsumerSubscription.Topics(topics), retryConfig = Some(retryConfig)),
  handler.withDeserializers(StringSerde, StringSerde)
  ).useForever
```

In order to avoid a lag build-up between producer and consumer for the partition on which a message is failing to process, there is a consumer API call to skip retrying for that partition:

```scala
import com.wixpress.dst.greyhound.core.consumer._

RecordConsumer.make(
  RecordConsumerConfig(...),
  handler
  ).flatMap(consumer => consumer.setBlockingState(IgnoreOnceFor(TopicPartition("topic-A", 0))))
```

## Producing via local disk

Greyhound offers a producer which writes records to local disk before it flushes them to Kafka. With this approach,
there will be definitely be some extra latency. But during longer outages, records will be safely stored locally before
they can be flushed.

```scala
import com.wixpress.dst.greyhound.core.producer._
import com.wixpress.dst.greyhound.core.producer.buffered._
import com.wixpress.dst.greyhound.core.producer.buffered.buffers._
import zio.duration._
import zio._

def producer: RManaged[ZEnv with GreyhoundMetrics, LocalBufferProducer[Any]] =
    for {
      producer <- Producer.make(ProducerConfig(brokers))
      h2LocalBuffer <- H2LocalBuffer.make(s"/writable/path/to/db/files", keepDeadMessages = 1.day)
      config = LocalBufferProducerConfig(maxMessagesOnDisk = 100000L, giveUpAfter = 1.hour,
        shutdownFlushTimeout = 2.minutes, retryInterval = 10.seconds,
        strategy = ProduceStrategy.Async(batchSize = 100, concurrency = 10))
      localDiskProducer <- LocalBufferProducer.make[Any](producer, h2LocalBuffer, config)
    } yield localDiskProducer
....
producer.use { p =>
  p.produce(record)
    .flatMap(_.kafkaResult
      .await
      .tap(res => zio.console.putStrLn(res.toString))
      .fork
    )
}
```

Notice that the effect of producing completes when the record has been persisted to Disk. The effect results with a
promise that fulfils with the Kafka produce metadata.

The producer will retry flushing failed records to Kafka in an interval defined by `retryInterval` config, until they expire according to
`giveUpAfter` config. Upon resource close, it will block until all records are flushed, limited to `shutdownFlushTimeout` config.

Use the `strategy` config to define how the producer flushes records to Kafka:

```scala
ProduceStrategy.Sync(concurrency: Int)
ProduceStrategy.Async(batchSize: Int, concurrency: Int)
ProduceStrategy.Unordered(batchSize: Int, concurrency: Int)
```

All of the strategies create N fibers (defined by `concurrency: Int`), grouped by keys or partitions. Each fiber is responsible
for flushing a range of targets (so there's no ordering or synchronization between different fibers).

- `ProduceStrategy.Sync` is the slowest strategy: it does not produce a record on a given key before the previous record has been acknowledged by Kafka.
  It will retry each record individually until successful, before continuing to the next record.
- `ProduceStrategy.Async` will produce a batch of records and wait for them all to complete. If some failed, it will retry the failures until successful.
- `ProduceStrategy.Unordered` is the same as Async, only it tries to produce to Kafka directly in the event of a local disk failure to append records.

## Testing

Use the embedded Kafka library to test your app:

```scala
import com.wixpress.dst.greyhound.testkit._

ManagedKafka.make(ManagedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)).use { kafka =>
  // Start producing and consuming messages,
  // configure broker address on producers and consumers to localhost:9092


  // outside of this scope Kafka will be shutdown.
}
```

This starts a real Kafka broker and Zookeeper instance on defined ports. Use those ports to access Kafka within the managed
scope.
