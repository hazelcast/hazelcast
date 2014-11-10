package com.hazelcast.console;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * A simulated load test
 */
public final class SimulateLoadTask implements Callable, Serializable, HazelcastInstanceAware {

    private static final long serialVersionUID = 1;

    private final int delay;
    private final int taskId;
    private final String latchId;
    private transient HazelcastInstance hz;

    public SimulateLoadTask(int delay, int taskId, String latchId) {
        this.delay = delay;
        this.taskId = taskId;
        this.latchId = latchId;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hz = hazelcastInstance;
    }

    @Override
    public Object call() throws Exception {
        try {
            Thread.sleep(delay * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        hz.getCountDownLatch(latchId).countDown();
        System.out.println("Finished task:" + taskId);
        return null;
    }
}
