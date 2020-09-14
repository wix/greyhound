package com.wixpress.dst.greyhound.getting.started;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@RestController
public class GreyhoundApplication {

	public static final	String BOOT_START_SERVERS = "kafka:29092";
	public static final	String TOPIC = "";
	public static final	String GROUP = "";

	@RequestMapping("/")
	public String home() {
		return "Hello Greyhound Application";
	}

	public static void main(String[] args) {
		SpringApplication.run(GreyhoundApplication.class, args);

//		CompletableFuture<ConsumerRecord<Integer, String>> future = new CompletableFuture<>();
//
//		GreyhoundConfig config = new GreyhoundConfig(BOOT_START_SERVERS, GreyhoundRuntime.Live());
//		GreyhoundProducerBuilder producerBuilder = new GreyhoundProducerBuilder(config);
//		GreyhoundConsumersBuilder consumersBuilder = new GreyhoundConsumersBuilder(config, new HashMap<>())
//				.withConsumer(
//						new GreyhoundConsumer<>(
//								new String[] { "a", "b" },
//								GROUP,
//								aBlockingRecordHandler(future::complete),
//								new IntegerDeserializer(),
//								new StringDeserializer(),
//								OffsetReset.Latest,
//								ErrorHandler.NoOp()));
//
//		try (GreyhoundConsumers ignored = consumersBuilder.build();
//			 GreyhoundProducer producer = producerBuilder.build()) {
//
//			producer.produce(
//					new ProducerRecord<>(TOPIC, 123, "hello world"),
//					new IntegerSerializer(),
//					new StringSerializer());
//
//			ConsumerRecord<Integer, String> consumed = future.get(30, TimeUnit.SECONDS);
//		}
	}
}

