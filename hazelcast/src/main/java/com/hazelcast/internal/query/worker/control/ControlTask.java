package com.hazelcast.internal.query.worker.control;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.internal.query.worker.WorkerTask;

public interface ControlTask extends WorkerTask {
    QueryId getQueryId();
}
