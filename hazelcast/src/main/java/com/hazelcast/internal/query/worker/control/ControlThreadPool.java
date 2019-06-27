package com.hazelcast.internal.query.worker.control;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.internal.query.worker.AbstractThreadPool;
import com.hazelcast.internal.query.worker.data.DataThreadPool;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.sql.impl.SqlServiceImpl;

/**
 * Control thread pool. Responsible for query initiation and cancel as well as for reactions on asynchronous
 * events which may affect query execution: migration, member leave.
 */
public class ControlThreadPool extends AbstractThreadPool<ControlWorker> {
    /** Prefix for thread names in this pool. */
    private static final String THREAD_PREFIX = "query-control-";

    /** Query service. */
    private final SqlServiceImpl service;

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Data pool. */
    private final DataThreadPool dataPool;

    public ControlThreadPool(SqlServiceImpl service, NodeEngine nodeEngine, int threadCnt,
        DataThreadPool dataPool) {
        super(THREAD_PREFIX, threadCnt);

        this.service = service;
        this.nodeEngine = nodeEngine;
        this.dataPool = dataPool;
    }

    @Override
    protected ControlWorker createWorker(int idx) {
        return new ControlWorker(service, nodeEngine, dataPool);
    }

    public void submit(ControlTask task) {
        QueryId queryId = task.getQueryId();

        if (queryId == null) {
            // Tasks without a query ID should be broadcasted to all stripes.
            for (int i = 0; i < workers.length; i++)
                getWorker(i).offer(task);
        }
        else {
            int stripe = Math.abs(queryId.hashCode() % getThreadCount());

            getWorker(stripe).offer(task);
        }
    }
}
