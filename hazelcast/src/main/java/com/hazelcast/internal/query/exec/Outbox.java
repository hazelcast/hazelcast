package com.hazelcast.internal.query.exec;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.query.QueryId;
import com.hazelcast.internal.query.QueryService;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.internal.query.io.SendBatch;
import com.hazelcast.internal.query.operation.QueryBatchOperation;
import com.hazelcast.spi.NodeEngine;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Outbox to a single receiver.
 */
public class Outbox extends Mailbox {

    private final QueryId queryId;
    private final NodeEngine nodeEngine;
    private final Member member;
    private final int batchSize;
    private List<Row> batch;

    private final int targetStripe;
    private int targetThread = -1; // TODO: Magic constant

    public Outbox(int edgeId, int stripe, QueryId queryId, NodeEngine nodeEngine, Member member, int batchSize,
        int targetStripe) {
        super(edgeId, stripe);

        this.queryId = queryId;
        this.nodeEngine = nodeEngine;
        this.member = member;
        this.targetStripe = targetStripe;
        this.batchSize = batchSize;
    }

    /**
     * Accept a row.
     *
     * @param row Row.
     * @return {@code True} if the outbox can accept more data.
     */
    public boolean onRow(Row row) {
        if (batch == null)
            batch = new LinkedList<>();

        batch.add(row);

        if (batch.size() >= batchSize)
            send(false);

        // TODO: Unbounded for now.
        return true;
    }

    public void close() {
        System.out.println(">>> OUTBOX [CLOSE]: " + this + ": " + batch.size());

        send(true);
    }

    private void send(boolean last) {
        List<Row> batch0 = batch;

        if (batch0 == null)
            batch0 = Collections.emptyList();

        QueryBatchOperation op = new QueryBatchOperation(
            queryId,
            getEdgeId(),
            nodeEngine.getLocalMember().getUuid(),
            getStripe(),
            getThread(),
            targetStripe,
            targetThread,
            new SendBatch(batch0, null, last)
        );

        // TODO: Exception handling.
        nodeEngine.getOperationService().invokeOnTarget(QueryService.SERVICE_NAME, op, member.getAddress());

        batch = null;
    }

    @Override
    public String toString() {
        return "Outbox {edgeId=" + getEdgeId() +
            ", sourceMember=" + nodeEngine.getLocalMember().getUuid() +
            ", sourceStripe=" + getStripe() +
            ", sourceThread=" + getThread() +
            ", targetMember=" + member.getUuid() +
            ", targetStripe=" + targetStripe +
            ", targetThread=" + targetThread +
            '}';
    }
}
