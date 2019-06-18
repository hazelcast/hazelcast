package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.worker.WorkerTask;

public interface DataTask extends WorkerTask {
    /**
     * @return ID of data thread.
     */
    int getThread();
}
