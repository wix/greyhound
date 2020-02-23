package com.wixpress.dst.greyhound.java;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

public class RecordHandlers {

    /**
     * Create a record handler from an async function (returns a `CompletableFuture`).
     */
    public static <K, V> RecordHandler<K, V> anAsyncRecordHandler(Function<ConsumerRecord<K, V>,  CompletableFuture<Void>> handle) {
        return handle::apply;
    }

    /**
     * Create a record handler which will execute the action on the given executor.
     */
    public static <K, V> RecordHandler<K, V> aSyncRecordHandler(Consumer<ConsumerRecord<K, V>> handle, Executor executor) {
        // TODO use ZIO's `effectBlocking` for these handlers?
        return record -> CompletableFuture.runAsync(() -> handle.accept(record), executor);
    }

    /**
     * Create a record handler which will execute the action on current thread.
     * @apiNote You should only use this if your handler is not blocking and doesn't perform I/O!
     */
    public static <K, V> RecordHandler<K, V> aSyncRecordHandler(Consumer<ConsumerRecord<K, V>> handle) {
        return record -> {
            handle.accept(record);
            return CompletableFuture.completedFuture(null);
        };
    }

}
