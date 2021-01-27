package com.wixpress.dst.greyhound.java;

import com.wixpress.dst.greyhound.java.testkit.DefaultEnvironment;
import com.wixpress.dst.greyhound.java.testkit.Environment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.wixpress.dst.greyhound.java.RecordHandlers.aBlockingRecordHandler;
import static org.junit.Assert.*;


public class GreyhoundBuilderTest {

    private static Environment environment;

    static String topic = "some-topic";
    static String maxParTopic = "some-topic2";
    static String customPropsTopic = "some-custom-props-topic3";

    static String group = "some-group";
    static String maxParGroup = "some-group2";
    static String customPropsGroup = "some-custom-props-group3";

    @BeforeClass
    public static void beforeAll() {
        environment = new DefaultEnvironment();
        environment.kafka().createTopic(new TopicConfig(topic, 8, 1));
        environment.kafka().createTopic(new TopicConfig(maxParTopic, 8, 1));
        environment.kafka().createTopic(new TopicConfig(customPropsTopic, 8, 1));
    }

    @AfterClass
    public static void afterAll() throws Exception {
        environment.close();
    }

    @Test
    public void convert_producer_record_partition_correctly_to_scala_for_null_partition() {
        com.wixpress.dst.greyhound.core.producer.ProducerRecord<Object, String> greyhoundRecord = GreyhoundProducerBuilder.toGreyhoundRecord(new ProducerRecord<>("some-topic", "some-value"));
        assertEquals(Option.empty(), greyhoundRecord.partition());
    }

    @Test
    public void produce_and_consume_messages_from_a_single_partition() throws Exception {
        int numOfMessages = 2;
        List<ConsumerRecord<Integer, String>> consumedRecords = new LinkedList<>();
        CountDownLatch lock = new CountDownLatch(numOfMessages);

        GreyhoundConfig config = new GreyhoundConfig(environment.kafka().bootstrapServers());
        GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
        GreyhoundConsumersBuilder consumersBuilder = new GreyhoundConsumersBuilder(config)
                .withConsumer(
                        GreyhoundConsumer.with(
                                topic,
                                group,
                                aBlockingRecordHandler(value -> {
                                    consumedRecords.add(value);
                                    lock.countDown();
                                }),
                                new IntegerDeserializer(),
                                new StringDeserializer())
                                .withOffsetReset(OffsetReset.Latest)
                                .withErrorHandler(ErrorHandler.NoOp())
                                .withMaxParallelism(1));

        try (GreyhoundConsumers ignored = consumersBuilder.build();
             GreyhoundProducer producer = producerBuilder.build()) {

            for (int i = 0; i < numOfMessages; i++) {
                CompletableFuture<OffsetAndMetadata> producerFuture = produce(producer, 123, "hello world" + i);
                producerFuture.join();
            }

            boolean consumedAll = lock.await(3000, TimeUnit.MILLISECONDS);

            assertTrue(consumedAll);

            for (int i = 0; i < numOfMessages; i++) {
                ConsumerRecord<Integer, String> consumed = consumedRecords.get(i);
                assertEquals(consumed.key(), Integer.valueOf(123));
                assertEquals(consumed.value(), "hello world" + i);
            }
        }
    }

    @Test
    public void consume_with_max_parallelism() throws Exception {
        int numOfMessages = 500;
        int waitInMillis = numOfMessages * 15;
        CountDownLatch lock = new CountDownLatch(numOfMessages);
        CountDownLatch lockMaxPar = new CountDownLatch(numOfMessages);

        Queue<ConsumerRecord<Integer, String>> consumedNoPar = new ConcurrentLinkedQueue<>();
        Queue<ConsumerRecord<Integer, String>> consumedMaxPar = new ConcurrentLinkedQueue<>();

        String messagePrefix = UUID.randomUUID().toString();
        GreyhoundConfig config = new GreyhoundConfig(environment.kafka().bootstrapServers());
        GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
        GreyhoundConsumersBuilder consumersBuilder =
                new GreyhoundConsumersBuilder(config)
                        .withConsumer(consumerWith(maxParGroup, lockMaxPar, consumedMaxPar, maxParTopic, messagePrefix, 8))
                        .withConsumer(consumerWith(group, lock, consumedNoPar, topic, messagePrefix, 1));

        try (GreyhoundConsumers ignored = consumersBuilder.build();
             GreyhoundProducer producer = producerBuilder.build()) {

            for (int i = 0; i < numOfMessages; i++) {
                String msg = messagePrefix+"-"+i;
                produceTo(producer, topic, msg);
                produceTo(producer, maxParTopic, msg);
            }

            lock.await(waitInMillis, TimeUnit.MILLISECONDS);
            lockMaxPar.await(waitInMillis, TimeUnit.MILLISECONDS);
            assertEquals(consumedNoPar.size(), consumedMaxPar.size());
        }
    }

    @Test
    public void configure_consumer_with_nonblocking_retry_policy() throws Exception {
        CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();
        ConcurrentLinkedQueue<ConsumerRecord<Integer, String>> invocations = new ConcurrentLinkedQueue<>();
        String group = "non-blocking-retry";
        int timesToFail = 3;
        GreyhoundProducer producer = producerFor(group,
                failingRecordHandler(invocations, timesToFail, future),
                RetryConfigBuilder.nonBlockingRetry(Arrays.asList(
                        Duration.of(1, ChronoUnit.SECONDS),
                        Duration.of(1, ChronoUnit.SECONDS),
                        Duration.of(1, ChronoUnit.SECONDS)
                )));

        producer.produce(
                new ProducerRecord<>(topic, 123, "foo"),
                new IntegerSerializer(),
                new StringSerializer());

        future.get(30, TimeUnit.SECONDS);
        assertEquals(invocations.size(), timesToFail + 1);
        assertConsumerRecord(invocations.remove(), 123, "foo", topic);
        assertConsumerRecord(invocations.remove(), 123, "foo", topic + "-" + group + "-retry-0");
        assertConsumerRecord(invocations.remove(), 123, "foo", topic + "-" + group + "-retry-1");
        assertConsumerRecord(invocations.remove(), 123, "foo", topic + "-" + group + "-retry-2");
    }

    @Test
    public void configure_consumer_with_finite_blocking_retry_policy() throws Exception {
        CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();
        ConcurrentLinkedQueue<ConsumerRecord<Integer, String>> invocations = new ConcurrentLinkedQueue<>();
        String group = "finite-blocking-retry";
        int timesToFail = 4;
        GreyhoundProducer producer = producerFor(group,
                failingRecordHandler(invocations, timesToFail, future),
                RetryConfigBuilder.finiteBlockingRetry(Arrays.asList(
                        Duration.of(1, ChronoUnit.SECONDS),
                        Duration.of(1, ChronoUnit.SECONDS),
                        Duration.of(1, ChronoUnit.SECONDS)
                )));

        producer.produce(new ProducerRecord<>(topic, 123, "foo"),
                new IntegerSerializer(),
                new StringSerializer()).whenComplete((val, err) ->
                producer.produce(new ProducerRecord<>(topic, 123, "bar"),
                        new IntegerSerializer(),
                        new StringSerializer())
        );

        future.get(30, TimeUnit.SECONDS);
        assertEquals(invocations.size(), timesToFail + 1);
        for (int i = 0; i < timesToFail; i++)
            assertConsumerRecord(invocations.remove(), 123, "foo", topic);
        assertConsumerRecord(invocations.remove(), 123, "bar", topic);
    }

    @Test
    public void configure_consumer_with_blocking_followed_by_nonblocking_retry_policy() throws Exception {
        CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();
        ConcurrentLinkedQueue<ConsumerRecord<Integer, String>> invocations = new ConcurrentLinkedQueue<>();
        String group = "blocking-nonblocking-retry";
        int timesToFail = 2;
        GreyhoundProducer producer = producerFor(group,
                failingRecordHandler(invocations, timesToFail, future),
                RetryConfigBuilder.blockingFollowedByNonBlockingRetry(
                        Collections.singletonList(Duration.of(1, ChronoUnit.SECONDS)),
                        Collections.singletonList(Duration.of(1, ChronoUnit.SECONDS))
                ));

        producer.produce(new ProducerRecord<>(topic, 123, "foo"),
                new IntegerSerializer(),
                new StringSerializer());

        future.get(30, TimeUnit.SECONDS);
        assertEquals(invocations.size(), timesToFail + 1);
        assertConsumerRecord(invocations.remove(), 123, "foo", topic);
        assertConsumerRecord(invocations.remove(), 123, "foo", topic);
        assertConsumerRecord(invocations.remove(), 123, "foo", topic + "-" + group + "-retry-0");
    }

    @Test
    public void configure_consumer_with_exponential_blocking_retry_policy_max_multiplications() throws Exception {
        CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();
        ConcurrentLinkedQueue<ConsumerRecord<Integer, String>> invocations = new ConcurrentLinkedQueue<>();
        String group = "exponential1-blocking-retry";
        int timesToFail = 3;
        GreyhoundProducer producer = producerFor(group,
                failingRecordHandler(invocations, timesToFail, future),
                RetryConfigBuilder.exponentialBackoffBlockingRetry(Duration.of(100, ChronoUnit.MILLIS), 2, 1, false));

        producer.produce(new ProducerRecord<>(topic, 123, "foo"),
                new IntegerSerializer(),
                new StringSerializer()).whenComplete((val, err) ->
                producer.produce(new ProducerRecord<>(topic, 123, "bar"),
                        new IntegerSerializer(),
                        new StringSerializer())
        );

        future.get(30, TimeUnit.SECONDS);
        assertEquals(invocations.size(), timesToFail + 1);
        for (int i = 0; i < timesToFail; i++)
            assertConsumerRecord(invocations.remove(), 123, "foo", topic);
        assertConsumerRecord(invocations.remove(), 123, "bar", topic);
    }

    @Test
    public void configure_consumer_with_exponential_blocking_retry_policy_max_interval() throws Exception {
        CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();
        ConcurrentLinkedQueue<ConsumerRecord<Integer, String>> invocations = new ConcurrentLinkedQueue<>();
        String group = "exponential2-blocking-retry";
        int timesToFail = 3;
        GreyhoundProducer producer = producerFor(group,
                failingRecordHandler(invocations, timesToFail, future),
                RetryConfigBuilder.exponentialBackoffBlockingRetry(Duration.of(100, ChronoUnit.MILLIS), Duration.of(350, ChronoUnit.MILLIS), 1, false));

        producer.produce(new ProducerRecord<>(topic, 123, "foo"),
                new IntegerSerializer(),
                new StringSerializer()).whenComplete((val, err) ->
                producer.produce(new ProducerRecord<>(topic, 123, "bar"),
                        new IntegerSerializer(),
                        new StringSerializer())
        );

        future.get(30, TimeUnit.SECONDS);
        assertEquals(invocations.size(), timesToFail + 1);
        for (int i = 0; i < timesToFail; i++)
            assertConsumerRecord(invocations.remove(), 123, "foo", topic);
        assertConsumerRecord(invocations.remove(), 123, "bar", topic);
    }

    @Test
    public void configure_consumer_with_timeout_custom_props() throws Exception {
        int numOfMessages = 2;
        List<ConsumerRecord<Integer, String>> consumedRecords = new LinkedList<>();
        CountDownLatch lock = new CountDownLatch(numOfMessages);

        GreyhoundConfig config = new GreyhoundConfig(environment.kafka().bootstrapServers(), new HashMap<String, String>() {
            {
                put("fetch.min.bytes", String.valueOf(Integer.MAX_VALUE));
                put("request.timeout.ms", String.valueOf(Integer.MAX_VALUE));
            }
        });
        GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
        GreyhoundConsumersBuilder consumersBuilder = new GreyhoundConsumersBuilder(config)
                .withConsumer(
                        GreyhoundConsumer.with(
                                customPropsTopic,
                                customPropsGroup,
                                aBlockingRecordHandler(value -> {
                                    consumedRecords.add(value);
                                    lock.countDown();
                                }),
                                new IntegerDeserializer(),
                                new StringDeserializer())
                                .withOffsetReset(OffsetReset.Latest)
                                .withErrorHandler(ErrorHandler.NoOp())
                                .withMaxParallelism(1));

        try (GreyhoundConsumers ignored = consumersBuilder.build();
             GreyhoundProducer producer = producerBuilder.build()) {

            for (int i = 0; i < numOfMessages; i++) {
                CompletableFuture<OffsetAndMetadata> producerFuture = producer.produce(
                        new ProducerRecord<>(customPropsTopic, 123, "custom props hello world" + i),
                        new IntegerSerializer(),
                        new StringSerializer());
                producerFuture.join();
            }

            boolean consumedAll = lock.await(2_000, TimeUnit.MILLISECONDS);

            assertFalse(consumedAll);
            assertFalse(consumedRecords.size() == numOfMessages);
        }
    }

    private void produceTo(GreyhoundProducer producer, String topic, String message) {
        producer.produce(
                new ProducerRecord<>(topic, message),
                new IntegerSerializer(),
                new StringSerializer());
    }

    private CompletableFuture<OffsetAndMetadata> produce(GreyhoundProducer producer, Integer key, String value) {
        return producer.produce(
                new ProducerRecord<>(topic, key, value),
                new IntegerSerializer(),
                new StringSerializer());
    }

    private GreyhoundConsumer<Integer, String> consumerWith(String group, CountDownLatch lockMaxPar,
                                                            Queue<ConsumerRecord<Integer, String>> consumed,
                                                            String topic2,
                                                            String msgPrefix,
                                                            int parallelism) {
        return GreyhoundConsumer.with(
                topic2,
                group,
                aBlockingRecordHandler(value -> {
                    if(value.value().startsWith(msgPrefix)) {
                        consumed.add(value);
                        lockMaxPar.countDown();
                    }
                }),
                new IntegerDeserializer(),
                new StringDeserializer())
                .withMaxParallelism(parallelism);
    }

    private GreyhoundProducer producerFor(String group, RecordHandler<Integer, String> handler, RetryConfig retryConfig) {
        GreyhoundConfig config = new GreyhoundConfig(environment.kafka().bootstrapServers());
        GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
        GreyhoundConsumersBuilder consumersBuilder = new GreyhoundConsumersBuilder(config)
                .withConsumer(GreyhoundConsumer.with(
                        topic,
                        group,
                        handler,
                        new IntegerDeserializer(),
                        new StringDeserializer())
                        .withRetryConfig(retryConfig));

        consumersBuilder.build();
        return producerBuilder.build();
    }

    private <K, V> RecordHandler<K, V> failingRecordHandler(ConcurrentLinkedQueue<ConsumerRecord<K, V>> invocations,
                                                            int timesToFail,
                                                            CompletableFuture<ConsumerRecord<K, V>> done) {
        return RecordHandlers.aBlockingRecordHandler(value -> {
            invocations.add(value);
            if (invocations.size() <= timesToFail) {
                throw new RuntimeException("Oops!");
            } else {
                done.complete(value);
            }
        });
    }

    private <K, V> void assertConsumerRecord(ConsumerRecord<K, V> record, K key, V value, String topic) {
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        assertEquals(topic, record.topic());
    }
}
