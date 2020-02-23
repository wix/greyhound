package com.wixpress.dst.greyhound.java;

public interface Greyhound extends AutoCloseable {

    /**
     * Create a producer according to spec.
     */
    GreyhoundProducer producer(GreyhoundProducerConfig config);

}
