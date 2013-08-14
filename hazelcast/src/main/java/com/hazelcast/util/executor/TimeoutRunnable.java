package com.hazelcast.util.executor;

import java.util.concurrent.TimeUnit;

public interface TimeoutRunnable extends Runnable {

    long getTimeout();

    TimeUnit getTimeUnit();
}
