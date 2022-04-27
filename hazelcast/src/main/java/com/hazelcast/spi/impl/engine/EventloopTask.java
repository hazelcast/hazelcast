package com.hazelcast.spi.impl.engine;

public interface EventloopTask {
    void run() throws Exception;
}
