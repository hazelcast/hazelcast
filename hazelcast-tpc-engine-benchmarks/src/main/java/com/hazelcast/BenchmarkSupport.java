package com.hazelcast;

import com.hazelcast.internal.tpc.Reactor;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BenchmarkSupport {

    public static final int TERMINATION_TIMEOUT_SECONDS = 30;

    public static void terminate(Reactor reactor) {
        if (reactor == null) {
            return;
        }

        reactor.shutdown();
        try {
            if (!reactor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
                throw new RuntimeException("Eventloop failed to terminate within timeout.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
