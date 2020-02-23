package com.wixpress.dst.greyhound.java.testkit;

import com.wixpress.dst.greyhound.java.TopicConfig;

import java.util.Set;

public interface ManagedKafka {

    Set<String> bootstrapServers();

    void createTopic(TopicConfig config);

}
