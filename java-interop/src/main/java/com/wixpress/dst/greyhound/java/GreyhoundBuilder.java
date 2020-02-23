package com.wixpress.dst.greyhound.java;

public interface GreyhoundBuilder {

    <K, V> GreyhoundBuilder withConsumer(GreyhoundConsumer<K, V> consumer);

    Greyhound build();

}
