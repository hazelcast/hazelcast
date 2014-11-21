package com.hazelcast.nio.tcp;

/**
 * The IOEventLoop is responsible for running the main IO event loop.
 *
 * In most cases, it will loop around receiving events from the selector/reactor and processing them.
 */
public interface IOEventLoop {

    /**
     * Runs this event loop. When the method returns, it means that the event loop should not be run anymore. E.g.
     * when the system is shutting down. So this method will only be called once.
     */
    void run();

    void dumpPerformanceMetrics(StringBuffer sb);
}
