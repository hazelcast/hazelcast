package com.hazelcast.internal.query.worker;

public class StopWorkerTask implements WorkerTask {

    public static final StopWorkerTask INSTANCE = new StopWorkerTask();

    private StopWorkerTask() {
        // No-op.
    }
}
