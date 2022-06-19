# Java API

Greyhound offers a Java API.

You can play with our Docker-based [quick-start Java App](../getting-started/README.md) that demonstrates how to use Greyhound.

## Configuration

Creating a blocking greyhound-batch-consumer is simple; You need to define the topic, and the group, and you need to implement the handle functionality of the BatchRecordHandler.

```java
import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecordBatch;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

Set<String> servers = Stream.of("localhost:9092").collect(Collectors.toCollection(HashSet::new));
GreyhoundConfig config = new GreyhoundConfig(servers);

GreyhoundBatchConsumer<String, String> batchConsumer = GreyhoundBatchConsumer.with(
        "some-topic",
        "some-consumer-group",
        BatchRecordHandlers.aBlockingBatchRecordHandler(
                new Consumer<ConsumerRecordBatch<String, String>>() {
                    @Override
                    public void accept(ConsumerRecordBatch<String, String> batchRecord) {
                        /* Your Handling here */
                    }
                }
        ),
        new StringDeserializer(),
        new StringDeserializer()
);

GreyhoundConsumersBuilder consumersBuilder = new GreyhoundConsumersBuilder(config)
        .withBatchConsumer(batchConsumer);

GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);

try (GreyhoundConsumers consumer = consumersBuilder.build(); // Start consuming
     GreyhoundProducer producer = producerBuilder.build()) // Create a producer
{
    producer.produce( // Produce to topic
            new ProducerRecord<>("some-topic", "hello world"),
            new StringSerializer(),
            new StringSerializer());

    // Shutdown all consumers and producers
    consumer.close();
    producer.close();
} catch (Exception e) {
    /* Handle Exceptions */
}
```

<br>
