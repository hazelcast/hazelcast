package com.hazelcast.internal.query.operation;

import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.internal.query.mailbox.SendBatch;
import com.hazelcast.internal.query.worker.data.BatchDataTask;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Execution batch.
 */
public class QueryBatchOperation extends QueryAbstractOperation {
    private QueryId queryId;
    private int edgeId;
    private String sourceMemberId;
    private int sourceStripe;
    private int sourceThread;
    private int targetStripe;
    private int targetThread;
    private SendBatch batch;

    public QueryBatchOperation() {
        // No-op.
    }

    public QueryBatchOperation(
        QueryId queryId,
        int edgeId,
        String sourceMemberId,
        int sourceStripe,
        int sourceThread,
        int targetStripe,
        int targetThread,
        SendBatch batch
    ) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.sourceMemberId = sourceMemberId;
        this.sourceStripe = sourceStripe;
        this.sourceThread = sourceThread;
        this.targetStripe = targetStripe;
        this.targetThread = targetThread;
        this.batch = batch;
    }

    @Override
    public void run() throws Exception {
        QueryService service = getService();

        service.onQueryBatchRequest(new BatchDataTask(queryId, edgeId, sourceMemberId, sourceStripe, sourceThread,
            targetStripe, targetThread, batch));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(queryId);
        out.writeInt(edgeId);
        out.writeUTF(sourceMemberId);
        out.writeInt(sourceStripe);
        out.writeInt(sourceThread);
        out.writeInt(targetStripe);
        out.writeInt(targetThread);

        batch.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        queryId = in.readObject();
        edgeId = in.readInt();
        sourceMemberId = in.readUTF();
        sourceStripe = in.readInt();
        sourceThread = in.readInt();
        targetStripe = in.readInt();
        targetThread = in.readInt();

        batch = new SendBatch();
        batch.readData(in);
    }
}
