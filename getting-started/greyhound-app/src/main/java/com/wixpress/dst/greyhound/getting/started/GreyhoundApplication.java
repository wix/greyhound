package com.wixpress.dst.greyhound.getting.started;

import com.wixpress.dst.greyhound.core.CleanupPolicy;
import com.wixpress.dst.greyhound.core.TopicConfig;
import com.wixpress.dst.greyhound.core.admin.AdminClientConfig;
import com.wixpress.dst.greyhound.future.AdminClient;
import com.wixpress.dst.greyhound.java.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.collection.immutable.HashMap;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.wixpress.dst.greyhound.java.RecordHandlers.aBlockingRecordHandler;

@SpringBootApplication
@RestController
public class GreyhoundApplication implements CommandLineRunner {

	public static final	String BOOT_START_SERVERS = "kafka:29092"; // This is the broker address in the docker
	public static final	String TOPIC = "greyhound-topic";
	public static final	String SLOW_GROUP = "greyhound-group-slow";
	public static final	String FAST_GROUP = "greyhound-group-fast";

	public static final	String BATCH_TOPIC = "greyhound-batch-topic";
	public static final	String BATCH_GROUP = "greyhound-batch-group-fast";
	public static final	String NON_BATCH_GROUP = "greyhound-non-batch-group";

	public static final	int PARTITIONS = 8; // The number of partitions is the number of max parallelism

	private final HashMap<String, String> EMPTY_MAP = new HashMap<>();

	private AdminClient adminClient;
	private GreyhoundProducer producer;
	private GreyhoundConsumers consumers;


	private long slowConsumerStartTime;
	private long fastConsumerStartTime;
	private AtomicInteger slowCounter;
	private AtomicInteger fastCounter;
	private AtomicBoolean slowConsumerStarted;
	private AtomicBoolean fastConsumerStarted;

	private long nonBatchConsumerStartTime;
	private long batchConsumerStartTime;
	private AtomicInteger nonBatchCounter;
	private AtomicInteger batchCounter;
	private AtomicBoolean nonBatchConsumerStarted;
	private AtomicBoolean batchConsumerStarted;

	private final OperationsRecorder nonBatchOperationsRecorder = new OperationsRecorder();
	private final OperationsRecorder batchOperationsRecorder = new OperationsRecorder();

	public GreyhoundApplication() {
	}

	/// Rest API ///
	@RequestMapping("/")
	public String home() {
		return "Hello Greyhound Application";
	}

	@RequestMapping("/produce")
	public String produce(@RequestParam("numOfMessages") int numOfMessages/*,
						  @RequestParam(value = "maxParallelism", defaultValue = "1") int maxParallelism*/) {
		// Save some details so we can measure time it took to consume all messages and reduce the parallelism
		slowCounter = new AtomicInteger(numOfMessages);
		fastCounter = new AtomicInteger(numOfMessages);
		slowConsumerStarted = new AtomicBoolean(false);
		fastConsumerStarted = new AtomicBoolean(false);

		String message = produceMessages(numOfMessages, TOPIC);

		System.out.println(message);
		return message;
	}

	@RequestMapping("/produce-batch")
	public String produceBatch(@RequestParam("numOfMessages") int numOfMessages) {
		// Save some details so we can measure time it took to consume all messages
		nonBatchCounter = new AtomicInteger(numOfMessages);
		batchCounter = new AtomicInteger(numOfMessages);
		nonBatchConsumerStarted = new AtomicBoolean(false);
		batchConsumerStarted = new AtomicBoolean(false);
		nonBatchOperationsRecorder.reset();
		batchOperationsRecorder.reset();

		String message = produceMessages(numOfMessages, BATCH_TOPIC);

		System.out.println(message);
		return message;
	}

	private String produceMessages(int numOfMessages, String topic) {
		long produceStart = System.currentTimeMillis();
		for (int i = 0; i < numOfMessages; i++) {
			producer.produce(
					new ProducerRecord<>(topic, i % 8, i, "message" + i),
					new IntegerSerializer(),
					new StringSerializer());
		}
		return "produced " + numOfMessages + " messages in " + (System.currentTimeMillis() - produceStart) + " millis at " + new Date(produceStart);
	}

	/// Application Startup ///
	public static void main(String[] args) {
		SpringApplication.run(GreyhoundApplication.class, args);
	}

	@Override
	public void run(String... args) {
		// Configure Greyhound
		GreyhoundConfig config = new GreyhoundConfig(BOOT_START_SERVERS);
		createTopics(new String[]{TOPIC, BATCH_TOPIC}); //Not necessary for topic with default configurations
		createProducer(config);
		createConsumers(config);
	}

	/// Greyhound Configurations ///
	private void createProducer(GreyhoundConfig config) {
		producer = new GreyhoundProducerBuilder(config).build();
	}

	private void createTopics(String[] topics) {
		adminClient = AdminClient.create(new AdminClientConfig(BOOT_START_SERVERS, EMPTY_MAP));
		for (String topic : topics) {
			adminClient.createTopic(new TopicConfig(
					topic,
					PARTITIONS,
					1,
					new CleanupPolicy.Delete(Duration.ofHours(1).toMillis()),
					EMPTY_MAP)).isCompleted();
		}
	}

	private void createConsumers(GreyhoundConfig config) {
		consumers = new GreyhoundConsumersBuilder(config)
				.withConsumer(
						aConsumer(SLOW_GROUP, 1))
				.withConsumer(
						aConsumer(FAST_GROUP, 8))
				.withConsumer(
						aNonBatchConsumer())
				.withBatchConsumer(
						aBatchConsumer())
				.build();
	}

	private GreyhoundConsumer<Integer, String> aConsumer(String group, int maxParallelism) {
		return GreyhoundConsumer.with(
				TOPIC,
				group,
				aBlockingRecordHandler(record -> {
					if (getStartedFlagBy(group).compareAndSet(false, true)) {
						setStartTimeBy(group);
						System.out.println("started consuming for '" + group + "'...");
					}
					int count = getCounterBy(group).decrementAndGet();
					artificialDelay();
					if (count == 0) {
						long lastConsumeTime = System.currentTimeMillis();
						System.out.println("----------------------------------------------------------------------");
						System.out.println("Consumer with MaxParallelism = " + maxParallelism + " - All messages consumed in "
								+ (lastConsumeTime - getStartTimeBy(group)) + " millis at " + new Date(lastConsumeTime));
					}
				}),
				new IntegerDeserializer(),
				new StringDeserializer())
				.withMaxParallelism(maxParallelism);
	}

	private GreyhoundConsumer<Integer, String> aNonBatchConsumer() {
		return GreyhoundConsumer.with(
				BATCH_TOPIC,
				NON_BATCH_GROUP,
				aBlockingRecordHandler(record -> {
					if (nonBatchConsumerStarted.compareAndSet(false, true)) {
						nonBatchConsumerStartTime = System.currentTimeMillis();
						System.out.println("started regular consumer processing...");
					}
					nonBatchOperationsRecorder.recordOperation(record.partition(), 1);
					artificialDelay();
					int count = nonBatchCounter.decrementAndGet();
					if (count == 0) {
						long lastConsumeTime = System.currentTimeMillis();
						System.out.println("----------------------------------------------------------------------");
						System.out.println("Regular consumer processed " + nonBatchOperationsRecorder.totalEntries() + " messages in " + nonBatchOperationsRecorder.totalOperations() + " operations, "
								+ "consumed in " + (lastConsumeTime - nonBatchConsumerStartTime) + " millis at " + new Date(lastConsumeTime) + "\n"
								+ "Operations per partition:\n"
								+ nonBatchOperationsRecorder.operationsPerPartitionEntries().stream().map(e -> "Partition " + e.getKey() + " - " + e.getValue().stream().reduce(0, Integer::sum)).collect(Collectors.joining(" operation(s) \n", "", " operation(s)")));
					}
				}),
				new IntegerDeserializer(),
				new StringDeserializer())
				.withMaxParallelism(PARTITIONS);
	}

	private GreyhoundBatchConsumer<Integer, String> aBatchConsumer() {
		return GreyhoundBatchConsumer.with(
				BATCH_TOPIC,
				BATCH_GROUP,
				BatchRecordHandlers.aBlockingBatchRecordHandler(batchRecord -> {
					if (batchConsumerStarted.compareAndSet(false, true)) {
						batchConsumerStartTime = System.currentTimeMillis();
						System.out.println("started batch consumer processing...");
					}
					int sizeOfBatch = batchRecord.records().size();
					batchOperationsRecorder.recordOperation(batchRecord.partition(), sizeOfBatch);
					artificialDelay();
					int count = batchCounter.updateAndGet(acc -> acc - sizeOfBatch);
					if (count == 0) {
						long lastConsumeTime = System.currentTimeMillis();
						System.out.println("----------------------------------------------------------------------");
						System.out.println("Batch consumer processed " + batchOperationsRecorder.totalEntries() + " messages in " + batchOperationsRecorder.totalOperations() + " operations, "
								+ "consumed in " + (lastConsumeTime - batchConsumerStartTime) + " millis at " + new Date(lastConsumeTime) + "\n"
								+ "Batch operations and sizes per partition:\n"
								+ batchOperationsRecorder.operationsPerPartitionEntries().stream().map(e -> "Partition " + e.getKey() + " - " + e.getValue().size() + " operation(s) with size(s): " + e.getValue()).collect(Collectors.joining("\n")));
					}
				}),
				new IntegerDeserializer(),
				new StringDeserializer()
		);
	}

	private AtomicInteger getCounterBy(String group) {
		if (group.equals(SLOW_GROUP))
			return slowCounter;
		else
			return fastCounter;
	}

	private AtomicBoolean getStartedFlagBy(String group) {
		if (group.equals(SLOW_GROUP))
			return slowConsumerStarted;
		else
			return fastConsumerStarted;
	}

	private void setStartTimeBy(String group) {
		if (group.equals(SLOW_GROUP))
			slowConsumerStartTime = System.currentTimeMillis();
		else
			fastConsumerStartTime = System.currentTimeMillis();
	}

	private long getStartTimeBy(String group) {
		if (group.equals(SLOW_GROUP))
			return slowConsumerStartTime;
		else
			return fastConsumerStartTime;
	}

	private void artificialDelay() {
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

