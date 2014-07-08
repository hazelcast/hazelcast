package com.hazelcast.client.executor.tasks;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class CancellationAwareTask implements Callable<Boolean>, Serializable {

    long sleepTime;

    public CancellationAwareTask(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public Boolean call() throws InterruptedException {
        Thread.sleep(sleepTime);
        return Boolean.TRUE;
    }
}

