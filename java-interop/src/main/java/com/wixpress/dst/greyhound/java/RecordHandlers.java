package com.wixpress.dst.greyhound.java;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class RecordHandlers {

    /**
     * Create a record handler from an async function (returns a `CompletableFuture`).
     */
    public static <K, V> RecordHandler<K, V> aNonBlockingRecordHandler(Function<ConsumerRecord<K, V>, CompletableFuture<Void>> handle) {
        return (record, executor) -> handle.apply(record);
    }

    /**
     * Create a record handler which will execute the action a executor designated for blocking code.
     */
    public static <K, V> RecordHandler<K, V> aBlockingRecordHandler(Consumer<ConsumerRecord<K, V>> handle) {
        return (record, executor) -> CompletableFuture.runAsync(() -> handle.accept(record), executor);
    }

    /**
     * Create a record handler which will execute the action on current thread.
     *
     * NOTE: This should NOT be used if your code involves blocking I/O!
     * TODO: rename
     */
    public static <K, V> RecordHandler<K, V> aDangerousHandler(Consumer<ConsumerRecord<K, V>> handle) {
        return (record, executor) -> {
            handle.accept(record);
            return CompletableFuture.completedFuture(null);
        };
    }

}
