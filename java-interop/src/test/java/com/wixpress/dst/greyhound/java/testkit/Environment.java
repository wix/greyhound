package com.wixpress.dst.greyhound.java.testkit;

public interface Environment extends AutoCloseable {

    ManagedKafka kafka();

}
