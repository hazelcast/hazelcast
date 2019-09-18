/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.mailbox;

import com.hazelcast.cluster.Member;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlTransientException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.operation.QueryBatchOperation;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.worker.data.DataWorker;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Outbox which sends data to a single remote stripe.
 */
public class Outbox extends AbstractMailbox {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Target member ID. */
    private final String targetMemberId;

    /** Target stripe. */
    private final int targetStripe;

    /** Batch size. */
    private final int batchSize;

    /** Target member. */
    private Member targetMember;

    /** Target thread. */
    private int targetThread = DataWorker.UNMAPPED_STRIPE;

    /** Pending rows.. */
    private List<Row> batch;

    public Outbox(int edgeId, int stripe, QueryId queryId, NodeEngine nodeEngine, String targetMemberId, int batchSize,
        int targetStripe) {
        super(queryId, edgeId, stripe);

        this.nodeEngine = nodeEngine;
        this.targetMemberId = targetMemberId;
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

        return true;
    }

    /**
     * Flush remaining rows.
     */
    public void flush() {
        send(true);
    }

    /**
     * Send rows to target member.
     *
     * @param last Whether this is the last batch.
     */
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
            new SendBatch(batch0, last)
        );

        try {
            if (targetMember == null)
                targetMember = nodeEngine.getClusterService().getMember(targetMemberId);

            nodeEngine.getOperationService().invokeOnTarget(SqlService.SERVICE_NAME, op, targetMember.getAddress());
        }
        catch (Exception e) {
            throw new HazelcastSqlTransientException(SqlErrorCode.MEMBER_LEAVE,
                "Failed to send data batch to member: " + this);
        }

        batch = null;
    }

    @Override
    public String toString() {
        return "Outbox {queryId=" + queryId +
            ", edgeId=" + getEdgeId() +
            ", stripe=" + getStripe() +
            ", thread=" + getThread() +
            ", targetMemberId=" + targetMemberId +
            ", targetStripe=" + targetStripe +
            ", targetThread=" + targetThread +
        '}';
    }
}
