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
import scala.Option;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.wixpress.dst.greyhound.java.RecordHandlers.aBlockingRecordHandler;
import static org.junit.Assert.assertEquals;


public class GreyhoundBuilderTest {

    private static Environment environment;

    static String topic = "some-topic";
    static String maxParTopic = "some-topic2";

    static String group = "some-group";
    static String maxParGroup = "some-group2";

    @BeforeClass
    public static void beforeAll() {
        environment = new DefaultEnvironment();
        environment.kafka().createTopic(new TopicConfig(topic, 8, 1));
        environment.kafka().createTopic(new TopicConfig(maxParTopic, 8, 1));
    }

    @AfterClass
    public static void afterAll() throws Exception {
        environment.close();
    }

    @Test
    public void convert_producer_record_partition_correctly_to_scala_for_null_partition() {
        com.wixpress.dst.greyhound.core.producer.ProducerRecord<Object, String> greyhoundRecord = GreyhoundProducerBuilder.toGreyhoundRecord(new ProducerRecord<>("some-topic", "some-value"));
        assertEquals(Option.empty(),greyhoundRecord.partition());
    }

    @Test
    public void produce_and_consume_a_single_message() throws Exception {
        CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();

        GreyhoundConfig config = new GreyhoundConfig(environment.kafka().bootstrapServers());
        GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
        GreyhoundConsumersBuilder consumersBuilder = new GreyhoundConsumersBuilder(config)
                .withConsumer(
                        GreyhoundConsumer.with(
                                topic,
                                group,
                                aBlockingRecordHandler(future::complete),
                                new IntegerDeserializer(),
                                new StringDeserializer())
                                .withOffsetReset(OffsetReset.Latest)
                                .withErrorHandler(ErrorHandler.NoOp())
                                .withMaxParallelism(1));

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

    @Test
    public void consume_faster_with_max_parallelism() throws Exception {
        int numOfMessages = 500;
        int waitInMillis = numOfMessages * 8;
        CountDownLatch lock = new CountDownLatch(numOfMessages);
        CountDownLatch lockMaxPar = new CountDownLatch(numOfMessages);

        Queue<ConsumerRecord<Integer, String>> consumedNoPar = new ConcurrentLinkedQueue<ConsumerRecord<Integer, String>>();
        Queue<ConsumerRecord<Integer, String>> consumedMaxPar = new ConcurrentLinkedQueue<ConsumerRecord<Integer, String>>();

        GreyhoundConfig config = new GreyhoundConfig(environment.kafka().bootstrapServers());
        GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
        GreyhoundConsumersBuilder consumersBuilder =
                new GreyhoundConsumersBuilder(config)
                        .withConsumer(consumerWith(group, lockMaxPar, consumedMaxPar, maxParTopic, 8))
                        .withConsumer(consumerWith(maxParGroup, lock, consumedNoPar, topic, 1));

        try (GreyhoundConsumers ignored = consumersBuilder.build();
             GreyhoundProducer producer = producerBuilder.build()) {

            for (int i = 0; i < numOfMessages; i++) {
                produceTo(producer, topic);
                produceTo(producer, maxParTopic);
            }

            lock.await(waitInMillis, TimeUnit.MILLISECONDS);
            lockMaxPar.await(waitInMillis, TimeUnit.MILLISECONDS);
            assertEquals(consumedNoPar.size(), consumedMaxPar.size());
        }
    }

    private void produceTo(GreyhoundProducer producer, String topic) {
        producer.produce(
                new ProducerRecord<>(topic, "hello world"),
                new IntegerSerializer(),
                new StringSerializer());
    }

    private GreyhoundConsumer<Integer, String> consumerWith(String group, CountDownLatch lockMaxPar,
                                                            Queue<ConsumerRecord<Integer, String>> consumedMaxPar,
                                                            String topic2,
                                                            int parallelism) {
        return GreyhoundConsumer.with(
                topic2,
                group,
                aBlockingRecordHandler(value -> {
                    consumedMaxPar.add(value);
                    lockMaxPar.countDown();
                }),
                new IntegerDeserializer(),
                new StringDeserializer())
                .withMaxParallelism(parallelism);
    }
}
