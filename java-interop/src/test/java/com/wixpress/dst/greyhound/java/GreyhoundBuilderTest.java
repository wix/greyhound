package com.wixpress.dst.greyhound.java;

import com.wixpress.dst.greyhound.java.testkit.DefaultEnvironment;
import com.wixpress.dst.greyhound.java.testkit.Environment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.wixpress.dst.greyhound.java.RecordHandlers.aBlockingRecordHandler;
import static org.junit.Assert.assertEquals;

public class GreyhoundBuilderTest {

    private static Environment environment;

    static String topic = "some-topic";

    static String group = "some-group";

    @BeforeClass
    public static void beforeAll() {
        environment = new DefaultEnvironment();
        environment.kafka().createTopic(new TopicConfig(topic, 8, 1));
    }

    @AfterClass
    public static void afterAll() throws Exception {
        environment.close();
    }

    @Test
    public void produce_and_consume_a_single_message() throws Exception {
        CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();

        GreyhoundConfig config = new GreyhoundConfig(environment.kafka().bootstrapServers());
        GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
        GreyhoundConsumersBuilder consumersBuilder = new GreyhoundConsumersBuilder(config)
                .withConsumer(
                        new GreyhoundConsumer<>(
                                topic,
                                group,
                                aBlockingRecordHandler(future::complete),
                                new IntegerDeserializer(),
                                new StringDeserializer()));

        try (GreyhoundConsumers ignored = consumersBuilder.build();
             GreyhoundProducer producer = producerBuilder.build()) {

            producer.produce(
                    new ProducerRecord<>(topic, 123, "hello world"),
                    new IntegerSerializer(),
                    new StringSerializer());

            ConsumerRecord<Integer, String> consumed = future.get(30, TimeUnit.SECONDS);

            assertEquals(consumed.key(), Integer.valueOf(123));
            assertEquals(consumed.value(), "hello world");
        }
    }

}
