package com.hazelcast.internal.query.worker.control;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.worker.WorkerTask;

public interface ControlTask extends WorkerTask {
    QueryId getQueryId();
}
