package com.wixpress.dst.greyhound.java;

import com.wixpress.dst.greyhound.core.consumer.retry.ZRetryConfig;
import scala.collection.JavaConverters;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class RetryConfig {
    private final List<Duration> blockingBackoffs;
    private final List<Duration> nonBlockingBackoffs;

    public RetryConfig(List<Duration> blockingBackoffs, List<Duration> nonBlockingBackoffs) {
        this.blockingBackoffs = blockingBackoffs;
        this.nonBlockingBackoffs = nonBlockingBackoffs;
    }

    public static RetryConfig nonBlockingRetry(List<Duration> nonBlockingBackoffs) {
        return new RetryConfig(Collections.emptyList(), nonBlockingBackoffs);
    }

    public static RetryConfig finiteBlockingRetry(List<Duration> blockingBackoffs) {
        return new RetryConfig(blockingBackoffs, Collections.emptyList());
    }

    public static RetryConfig blockingFollowedByNonBlockingRetry(List<Duration> blockingBackoffs, List<Duration> nonBlockingBackoffs) {
        return new RetryConfig(blockingBackoffs, nonBlockingBackoffs);
    }

    public static RetryConfig exponentialBackoffBlockingRetry(Duration initialInterval,
                                                              Duration maximalInterval,
                                                              float backOffMultiplier,
                                                              boolean infiniteRetryMaxInterval) {
        return fromCoreRetryConfig(ZRetryConfig.exponentialBackoffBlockingRetry(initialInterval, maximalInterval, backOffMultiplier, infiniteRetryMaxInterval));
    }

    public static RetryConfig exponentialBackoffBlockingRetry(Duration initialInterval,
                                                              int maxMultiplications,
                                                              float backOffMultiplier,
                                                              boolean infiniteRetryMaxInterval) {
        return fromCoreRetryConfig(ZRetryConfig.exponentialBackoffBlockingRetry(initialInterval, maxMultiplications, backOffMultiplier, infiniteRetryMaxInterval));
    }

    public List<Duration> blockingBackoffs() {
        return blockingBackoffs;
    }

    public List<Duration> nonBlockingBackoffs() {
        return nonBlockingBackoffs;
    }

    private static RetryConfig fromCoreRetryConfig(com.wixpress.dst.greyhound.core.consumer.retry.RetryConfig coreRetryConfig) {
        List<Duration> blocking = JavaConverters.seqAsJavaList(coreRetryConfig.blockingBackoffs().apply());
        List<Duration> nonBlocking = JavaConverters.seqAsJavaList(coreRetryConfig.nonBlockingBackoffs());
        return new RetryConfig(blocking, nonBlocking);
    }
}
