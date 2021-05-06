package com.wixpress.dst.greyhound.java;

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecordBatch;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface BatchRecordHandler<K, V> {

    CompletableFuture<Void> handle(ConsumerRecordBatch<K, V> recordBatch, Executor executor);

}
