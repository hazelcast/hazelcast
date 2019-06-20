package com.hazelcast.internal.query.worker.data;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.io.SendBatch;
import com.hazelcast.internal.query.worker.control.ControlTask;

/**
 * Batch with unmapped threads arrived.
 */
public class BatchDataTask implements ControlTask, DataTask {

    private final QueryId queryId;
    private final int edgeId;
    private final int sourceStripe;
    private final int sourceThread;
    private final int targetStripe;
    private int targetThread;
    private final SendBatch batch;

    public BatchDataTask(QueryId queryId, int edgeId, int sourceStripe, int sourceThread, int targetStripe,
        int targetThread, SendBatch batch) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.sourceStripe = sourceStripe;
        this.sourceThread = sourceThread;
        this.targetStripe = targetStripe;
        this.targetThread = targetThread;
        this.batch = batch;
    }

    public boolean isMapped() {
        // TODO: Magic variable.
        return getTargetThread() != -1;
    }

    @Override
    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public int getThread() {
        return getTargetThread();
    }

    public int getEdgeId() {
        return edgeId;
    }

    public int getSourceStripe() {
        return sourceStripe;
    }

    public int getSourceThread() {
        return sourceThread;
    }

    public int getTargetStripe() {
        return targetStripe;
    }

    public int getTargetThread() {
        return targetThread;
    }

    public void setTargetThread(int targetThread) {
        this.targetThread = targetThread;
    }

    public SendBatch getBatch() {
        return batch;
    }
}