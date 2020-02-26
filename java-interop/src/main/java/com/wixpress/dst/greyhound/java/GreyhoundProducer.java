package com.wixpress.dst.greyhound.java;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.concurrent.CompletableFuture;

public interface GreyhoundProducer extends AutoCloseable {

    /**
     * Produce a record to Kafka using the provided serializers to serialize the key and value.
     */
    <K, V> CompletableFuture<OffsetAndMetadata> produce(ProducerRecord<K, V> record,
                                                        Serializer<K> keySerializer,
                                                        Serializer<V> valueSerializer);

}
