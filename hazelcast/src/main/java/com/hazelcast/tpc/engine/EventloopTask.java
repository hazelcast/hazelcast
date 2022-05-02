package com.hazelcast.tpc.engine;

public interface EventloopTask {
    void run() throws Exception;
}
