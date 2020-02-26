package com.wixpress.dst.greyhound.java;

public interface GreyhoundConsumers extends AutoCloseable {

    void pause();

    void resume();

    boolean isAlive();

}
