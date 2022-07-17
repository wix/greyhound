# Scala Future API

The basic Future API is less powerful than the [ZIO API](./zio-based-api.md), but it's a quick way to get started
without prior knowledge of effect systems.

## Configuration

```scala
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecord
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
      initialTopics = Set("some-topic"),
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
      valueDeserializer = Serdes.StringSerde,
      clientId = "client-id-1"))

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

## Using a custom metrics reporter

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
