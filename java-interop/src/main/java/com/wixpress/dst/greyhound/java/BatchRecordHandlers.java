package com.wixpress.dst.greyhound.java;

import com.wixpress.dst.greyhound.core.consumer.domain.ConsumerRecordBatch;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class BatchRecordHandlers {
    /**
     * Create a batch record handler from an async function (returns a `CompletableFuture`).
     */
    public static <K, V> BatchRecordHandler<K, V> aNonBlockingBatchRecordHandler(Function<ConsumerRecordBatch<K, V>, CompletableFuture<Void>> handle) {
        return (record, executor) -> handle.apply(record);
    }

    /**
     * Create a batch record handler from an async function (returns a `CompletableFuture`).
     */
    public static <K, V> BatchRecordHandler<K, V> aBlockingBatchRecordHandler(Consumer<ConsumerRecordBatch<K, V>> handle) {
        return (record, executor) -> CompletableFuture.runAsync(() -> handle.accept(record), executor);
    }

    /**
     * Create a batch record handler which will execute the action on current thread.
     *
     * NOTE: This should NOT be used if your code involves blocking I/O!
     */
    public static <K, V> BatchRecordHandler<K, V> sameThreadBatchRecordHandler(Consumer<ConsumerRecordBatch<K, V>> handle) {
        return (record, executor) -> {
            handle.accept(record);
            return CompletableFuture.completedFuture(null);
        };
    }
}
