package com.hazelcast.internal.query.worker.control;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.internal.query.worker.AbstractThreadPool;
import com.hazelcast.internal.query.worker.data.DataThreadPool;

public class ControlThreadPool extends AbstractThreadPool<ControlWorker> {

    private static final String THREAD_PREFIX = "query-control-";
    private static final int BROADCAST_STRIPE = -1;

    private final QueryService queryService;
    private final DataThreadPool dataPool;

    public ControlThreadPool(QueryService queryService, int threadCnt, DataThreadPool dataPool) {
        super(THREAD_PREFIX, threadCnt);

        this.queryService = queryService;
        this.dataPool = dataPool;
    }

    @Override
    protected ControlWorker createWorker(int idx) {
        return new ControlWorker(queryService, dataPool);
    }

    public void submit(ControlTask task) {
        int stripe = resolveStripe(task);

        if (stripe == BROADCAST_STRIPE) {
            for (int i = 0; i < workers.length; i++)
                getWorker(i).offer(task);
        }
        else
            getWorker(stripe).offer(task);
    }

    private int resolveStripe(ControlTask task) {
        QueryId queryId = task.getQueryId();

        if (queryId == null)
            return BROADCAST_STRIPE;
        else
            return queryId.hashCode() % getThreadCount();
    }
}
