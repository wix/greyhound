package com.wixpress.dst.greyhound.java;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface RecordHandler<K, V> {

    CompletableFuture<Void> handle(ConsumerRecord<K, V> record, Executor executor);

}
