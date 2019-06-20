package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.worker.AbstractThreadPool;

public class DataThreadPool extends AbstractThreadPool<DataWorker> {
    private static final int BROADCAST_STRIPE = -1; // TODO: Duplication
    private static final String THREAD_PREFIX = "query-data-";

    public DataThreadPool(int threadCnt) {
        super(THREAD_PREFIX, threadCnt);
    }

    @Override
    protected DataWorker createWorker(int idx) {
        return new DataWorker(this, idx);
    }

    public void submit(DataTask task) {
        int stripe = resolveStripe(task);

        if (stripe == BROADCAST_STRIPE) {
            for (int i = 0; i < workers.length; i++)
                getWorker(i).offer(task);
        }
        else
            getWorker(stripe).offer(task);
    }

    private int resolveStripe(DataTask task) {
        int thread = task.getThread();

        if (thread == -1) // TODO: Magic constant
            return BROADCAST_STRIPE;
        else
            return thread % getThreadCount();
    }

    public int getStripeCount() {
        return workers.length;
    }
}