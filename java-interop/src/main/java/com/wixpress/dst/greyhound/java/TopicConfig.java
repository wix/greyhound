package com.wixpress.dst.greyhound.java;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public class TopicConfig {

    private String name;

    private int partitions;

    private int replicationFactor;

    private CleanupPolicy cleanupPolicy;

    private Map<String, String> extraProperties;

    public TopicConfig(String name, int partitions, int replicationFactor, CleanupPolicy cleanupPolicy, Map<String, String> extraProperties) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.cleanupPolicy = cleanupPolicy;
        this.extraProperties = extraProperties;
    }

    public TopicConfig(String name, int partitions, int replicationFactor) {
        this(name, partitions, replicationFactor, CleanupPolicy.delete(Duration.ofHours(1)), Collections.emptyMap());
    }

    public String name() {
        return name;
    }

    public int partitions() {
        return partitions;
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    public CleanupPolicy cleanupPolicy() {
        return cleanupPolicy;
    }

    public Map<String, String> extraProperties() {
        return extraProperties;
    }

}
