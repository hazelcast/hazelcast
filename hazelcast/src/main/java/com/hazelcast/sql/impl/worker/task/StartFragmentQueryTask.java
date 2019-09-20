package com.hazelcast.sql.impl.worker.task;

import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.QueryResultConsumer;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.worker.task.QueryTask;

/**
 * A task to start fragment execution.
 */
public class StartFragmentQueryTask implements QueryTask {
    /** Operation. */
    private final QueryExecuteOperation operation;

    /** Fragment descriptor. */
    private final QueryFragmentDescriptor fragment;

    /** Query result consumer. */
    private final QueryResultConsumer consumer;

    public StartFragmentQueryTask(
        QueryExecuteOperation operation,
        QueryFragmentDescriptor fragment,
        QueryResultConsumer consumer
    ) {
        this.operation = operation;
        this.fragment = fragment;
        this.consumer = consumer;
    }

    public QueryExecuteOperation getOperation() {
        return operation;
    }

    public QueryFragmentDescriptor getFragment() {
        return fragment;
    }

    public QueryResultConsumer getConsumer() {
        return consumer;
    }
}
