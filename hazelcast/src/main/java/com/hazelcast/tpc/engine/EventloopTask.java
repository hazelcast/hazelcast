package com.hazelcast.tpc.engine;

/**
 * A task that gets executed on the {@link Eventloop}.
 */
public interface EventloopTask {

    void run() throws Exception;
}
