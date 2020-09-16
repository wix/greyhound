package com.wixpress.dst.greyhound.getting.started;

import com.wixpress.dst.greyhound.core.CleanupPolicy;
import com.wixpress.dst.greyhound.core.TopicConfig;
import com.wixpress.dst.greyhound.core.admin.AdminClientConfig;
import com.wixpress.dst.greyhound.future.AdminClient;
import com.wixpress.dst.greyhound.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.wixpress.dst.greyhound.java.RecordHandlers.aBlockingRecordHandler;

@SpringBootApplication
@RestController
public class GreyhoundApplication implements CommandLineRunner {

	public static final	String BOOT_START_SERVERS = "kafka:29092";
	public static final	String TOPIC = "greyhound-topic";
	public static final	String GROUP = "greyhound-group";

	private final HashMap<String, String> EMPTY_MAP = new HashMap<>();

	private AdminClient adminClient;
	private GreyhoundProducer producer;
	private GreyhoundConsumers consumers;

	private int currentNumOfMessages;
	private long produceStartTime;
	private long lastConsumeTime;
	private AtomicInteger counter;
	private Semaphore semaphore;

	public GreyhoundApplication() {
	}

	@RequestMapping("/")
	public String home() {
		return "Hello Greyhound Application";
	}

	@RequestMapping("/produce")
	public String produce(@RequestParam("numOfMessages") int numOfMessages,
						  @RequestParam(value = "maxParallelism", defaultValue = "1") int maxParallelism) {
		currentNumOfMessages = numOfMessages;
		counter = new AtomicInteger(currentNumOfMessages);
		semaphore = new Semaphore(maxParallelism);
		produceStartTime = System.currentTimeMillis();

		for (int i=0;i<numOfMessages;i++) {
			producer.produce(
					new ProducerRecord<>(TOPIC, i, "message"+i),
					new IntegerSerializer(),
					new StringSerializer());
		}

		return "produced " + numOfMessages + " messages at " + produceStartTime;
	}

	public static void main(String[] args) {
		SpringApplication.run(GreyhoundApplication.class, args);
	}

	@Override
	public void run(String... args) {
		GreyhoundConfig config = new GreyhoundConfig(BOOT_START_SERVERS);
		createTopic(); //Not necessary for topic with default configurations
		createProducer(config);
		createConsumer(config);
	}

	private void createConsumer(GreyhoundConfig config) {
		consumers = new GreyhoundConsumersBuilder(config)
				.withConsumer(
						new GreyhoundConsumer<>(
								TOPIC,
								GROUP,
								aBlockingRecordHandler(getConsumer()),
								new IntegerDeserializer(),
								new StringDeserializer(),
								OffsetReset.Latest,
								ErrorHandler.NoOp())).build();
	}

	private void createProducer(GreyhoundConfig config) {
		producer = new GreyhoundProducerBuilder(config).build();
	}

	private void createTopic() {
		adminClient = AdminClient.create(new AdminClientConfig(BOOT_START_SERVERS, EMPTY_MAP));
		adminClient.createTopic(new TopicConfig(
				TOPIC,
				1,
				1,
				new CleanupPolicy.Delete(Duration.ofHours(1).toMillis()),
				EMPTY_MAP));
	}

	private Consumer<ConsumerRecord<Integer, String>> getConsumer() {
		return record -> {
			try {
				semaphore.acquire();
				int count = counter.decrementAndGet();
//				System.out.println("Consumed message \"" + record.key() + " : " + record.value() + "\" " + count + " messages remains");
				if (count == 0) {
					lastConsumeTime = System.currentTimeMillis();
					System.out.println("Consumed all messages in " + (lastConsumeTime - produceStartTime) + " millis");
				}
				semaphore.release();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		};
	}
}

